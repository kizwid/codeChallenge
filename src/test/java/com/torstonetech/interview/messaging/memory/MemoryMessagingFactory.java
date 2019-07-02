package com.torstonetech.interview.messaging.memory;

import com.torstonetech.interview.messaging.Message;
import com.torstonetech.interview.messaging.MessageReceiveListener;
import com.torstonetech.interview.messaging.MessageReceiver;
import com.torstonetech.interview.messaging.MessageSender;
import com.torstonetech.interview.messaging.MessagingException;
import com.torstonetech.interview.messaging.MessagingFactory;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import lombok.extern.log4j.Log4j2;

import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

@Log4j2
public class MemoryMessagingFactory implements MessagingFactory {

    private final AtomicInteger receiverCounter = new AtomicInteger();
    Map<String, TopicNotifier> topicNotifiers = new ConcurrentHashMap<>();

    @Override
    public String getProviderName() {
        return "InMemory";
    }

    @Override
    public void shutdown() throws Exception {
        synchronized (topicNotifiers) {
            for (Map.Entry<String, TopicNotifier> entry : topicNotifiers.entrySet()) {
                entry.getValue().shutdown();
            }
            topicNotifiers.clear();
            receiverCounter.set(0);
        }

    }

    @Override
    public MessageSender createSender(final String topic) throws MessagingException {
        TopicNotifier topicNotifier = getTopicNotifier(topic);
        return InMemMessageSender.of(topic, topicNotifier.queue);
    }

    //the idea here is to have just one queue per topic which is shared between MessageSenders and MessageReceivers
    private TopicNotifier getTopicNotifier(String topic) {
        TopicNotifier topicNotifier = topicNotifiers.computeIfAbsent(topic, k -> TopicNotifier.of(topic, new LinkedBlockingQueue(), new CopyOnWriteArraySet<>()));
        if(!topicNotifier.isAlive()){
            topicNotifier.start(); //start notifying and clearing messages asap (not all senders have receivers)
        }
        return topicNotifier;
    }

    @Override
    public MessageReceiver createReceiver(final String topic) throws MessagingException {
        TopicNotifier topicNotifier = getTopicNotifier(topic);
        InMemMessageReceiver receiver = InMemMessageReceiver.of(topic, new CopyOnWriteArrayList<>(), receiverCounter.incrementAndGet());
        topicNotifier.addReceiver(receiver);
        return receiver;
    }

    /**
     * Method which can be called by unit test code to wait for all messages to be delivered and processed.
     * This should cater for message receivers which themselves send further messages, i.e. also waiting
     * for such subsequent messages to be consumed.
     *
     * @param timeoutMillis Overall timeout to wait for the messaging infrastructure to become idle.
     * @throws Exception If something goes wrong whilst waiting, or messages are still waiting to or being
     *                   handled when the timeout expires.
     */
    public void waitForMessages(final long timeoutMillis) throws Exception {
        final CountDownLatch latch = new CountDownLatch(1);
        //synchronized (topicNotifiers) {
            Executors.newFixedThreadPool(4).submit(new Runnable() {
                @Override
                public void run() {
                    try {
                        for (Map.Entry<String, TopicNotifier> entry : topicNotifiers.entrySet()) {
                            @NonNull BlockingQueue<Message> queue = entry.getValue().getQueue();
                            while (queue.size() > 0) {
                                log.info("Waiting for {} messages on topic {}", queue.size(), entry.getKey());
                                Thread.sleep(20);
                            }
                        }
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                    latch.countDown();
                }
            });
        //}
        latch.await(timeoutMillis, TimeUnit.MILLISECONDS);
    }

    @Data
    @AllArgsConstructor(staticName = "of")
    private static class InMemMessageSender implements MessageSender {
        String topic;
        @ToString.Exclude
        BlockingQueue<Message> queue;
        @Override
        public void sendMessage(byte[] message) throws MessagingException {
            queue.offer(ByteArrayMessage.of(Arrays.copyOf(message, message.length)));
        }
    }

    @Data
    @AllArgsConstructor(staticName = "of")
    private static class ByteArrayMessage implements Message {
        byte[] msg;
        @Override
        public void dispose() {
            msg = null;
        }
    }

    @Data
    @AllArgsConstructor(staticName = "of")
    private static class InMemMessageReceiver implements MessageReceiver {
        String topic;
        @ToString.Exclude
        CopyOnWriteArrayList<MessageReceiveListener> listeners;
        int uid;
        @Override
        public void setListener(MessageReceiveListener listener) {
            listeners.add(listener);
        }
        @Override
        public String toString() {
            return "InMemMessageReceiver{" + uid + "/" + topic +
                    '}';
        }
    }

    @Data
    @RequiredArgsConstructor(staticName = "of")
    @Log4j2
    private static class TopicNotifier extends Thread{
        @NonNull
        String topic;
        @NonNull
        @ToString.Exclude
        BlockingQueue<Message> queue;
        @NonNull
        CopyOnWriteArraySet<InMemMessageReceiver> receivers;

        private void addReceiver(InMemMessageReceiver receiver){
            receivers.add(receiver);
        }

        public void shutdown(){
            //shutdown=true;
            queue.add(new PoisonMessage());
        }

        @Override
        public void run() {
            super.run();
            log.info("Started TopicNotifier[" + this + "]");
            while (true) {
                try {
                    Message message = queue.take();
                    if(message instanceof PoisonMessage){
                        log.info("Notifier[{}] is shutdown", topic);
                        return;
                    }
                    for (InMemMessageReceiver receiver : receivers) {
                        for (MessageReceiveListener listener : receiver.getListeners()) {
                            try {
                                byte[] bytes = message.getMsg();
                                log.debug("sending msg[" + new String(bytes) + "] from receiver[" + receiver + "] to listener[" + listener + "]"); //if I slow things down by logging info here we see a threading bug
                                Message clone = ByteArrayMessage.of(Arrays.copyOf(bytes, bytes.length));
                                listener.onMessage(clone, topic);
                            } catch (Throwable t) {
                                log.error("Listener: {} failed to handle message: {}", listener, new String(message.getMsg()), t);
                            }
                        }
                    }
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    log.error("TopicNotifier[{}] interupted", topic, e);
                }
            }
        }


    }

    private static class PoisonMessage implements Message{
        @Override
        public byte[] getMsg() {
            return new byte[0];
        }
        @Override
        public void dispose() {

        }
    }

}
