package com.torstonetech.interview.messaging.memory;

import com.torstonetech.interview.messaging.*;
import lombok.ToString;
import org.testng.annotations.AfterSuite;
import org.testng.annotations.AfterTest;
import org.testng.annotations.BeforeSuite;

import java.util.ArrayList;
import java.util.List;

import static org.testng.AssertJUnit.assertEquals;

public abstract class MemoryMessagingTestBase {
  private MemoryMessagingFactory messagingFactory;

  @BeforeSuite
  public void before() throws Exception {
    messagingFactory = new MemoryMessagingFactory();
  }

  @AfterSuite
  public void after() throws Exception {
    messagingFactory.shutdown();
  }

  @AfterTest
  public void afterTest() throws Exception {
    //messagingFactory.shutdown();
  }

  // -------------------------------------------------------------------------------- //

  MessageSender newSender(final String topic) throws MessagingException {
    return messagingFactory.createSender(topic);
  }

  StoringListener newReceiver(final String topic) throws MessagingException {
    final StoringListener listener = new StoringListener();
    newReceiver(topic, listener);
    return listener;
  }

  MessageReceiver newReceiver(final String topic, final MessageReceiveListener listener) throws MessagingException {
    final MessageReceiver receiver = messagingFactory.createReceiver(topic);
    receiver.setListener(listener);
    return receiver;
  }

  void waitForMessages() throws Exception {
    messagingFactory.waitForMessages(5000);
  }

  static void checkMessages(final StoringListener listener, final String... messages) {
    assertEquals(messages.length, listener.messages.size());
    for (int i = 0; i < messages.length; ++i)
      assertEquals("Message " + i + " different", messages[i], decode(listener.messages.get(i)));
  }

  static void checkTopics(final StoringListener listener, final String... topics) {
    assertEquals(topics.length, listener.topics.size());
    for (int i = 0; i < topics.length; ++i)
      assertEquals("Topic " + i + " different", topics[i], listener.topics.get(i));
  }

  static void checkTopics(final StoringListener listener, final String topic, final int num) {
    assertEquals(num, listener.topics.size());
    for (int i = 0; i < num; ++i)
      assertEquals("Topic " + i + " different", topic, listener.topics.get(i));
  }

  static byte[] encode(final String string) {
    return string.getBytes(); //should we use UTF-8 encoding?
  }

  static String decode(final Message message) {
    return new String(message.getMsg()); //UTF-8 encoding?
  }

  protected static class StoringListener implements MessageReceiveListener {
    final List<Message> messages = new ArrayList<>();
    final List<String> topics = new ArrayList<>();

    @Override
    public void onMessage(final Message message, final String topic) {
      //synchronized (messages) {
        messages.add(message); //should these 2 operations be atomic?
        topics.add(topic);
      //}
    }

    @Override
    public String toString() {
      return "StoringListener@" + Integer.toHexString(System.identityHashCode(this)); //just wanted to see a short differenciator
    }
  }
}
