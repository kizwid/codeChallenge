package com.torstonetech.interview.messaging.memory;

import com.torstonetech.interview.messaging.MessageSender;
import com.torstonetech.interview.messaging.MessagingException;
import org.testng.annotations.Test;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.testng.Assert.*;

/**
 * Basic set of tests to check the messaging implementation works. More are probably necessary to check things such as proper
 * encapsulation and isolation etc.
 */
public class MemoryMessagingTest extends MemoryMessagingTestBase {
  @Test
  public void simpleSendReceive() throws Exception {
    final MessageSender sender = newSender("a");
    final StoringListener listener = newReceiver("a");

    sender.sendMessage(encode("The"));
    sender.sendMessage(encode("cat"));
    sender.sendMessage(encode("sat"));
    sender.sendMessage(encode("on"));
    sender.sendMessage(encode("the"));
    sender.sendMessage(encode("mat"));

    waitForMessages();
    checkTopics(listener, "a", 6);
    checkMessages(listener, "The", "cat", "sat", "on", "the", "mat");
  }

  @Test
  public void manySendersSameTopic() throws Exception {
    final MessageSender sender1 = newSender("a");
    final MessageSender sender2 = newSender("a");
    final MessageSender sender3 = newSender("a");
    final StoringListener listener = newReceiver("a");

    sender1.sendMessage(encode("The"));
    sender1.sendMessage(encode("cat"));
    sender2.sendMessage(encode("sat"));
    sender3.sendMessage(encode("on"));
    sender3.sendMessage(encode("the"));
    sender2.sendMessage(encode("mat"));

    waitForMessages();

    // all messages received as all senders on the same topic
    checkTopics(listener, "a", 6);
    checkMessages(listener, "The", "cat", "sat", "on", "the", "mat");
  }

  @Test
  public void manySendersDifferentTopics() throws Exception {
    final MessageSender sender1 = newSender("a");
    final MessageSender sender2 = newSender("b");
    final MessageSender sender3 = newSender("c");
    final StoringListener listener = newReceiver("b");

    sender1.sendMessage(encode("The"));
    sender1.sendMessage(encode("cat"));
    sender2.sendMessage(encode("sat"));
    sender3.sendMessage(encode("on"));
    sender3.sendMessage(encode("the"));
    sender2.sendMessage(encode("mat"));

    waitForMessages();

    // only messages sent on topic "b" will be received
    checkTopics(listener, "b", 2);
    checkMessages(listener, "sat", "mat");
  }

  @Test
  public void manyReceiversSameListenerSameTopic() throws Exception {
    final MessageSender sender = newSender("a");
    final StoringListener listener = new StoringListener();
    newReceiver("a", listener);
    newReceiver("a", listener);
    newReceiver("a", listener);

    // each listener is asynchronous, so have to wait between each send for messages to finish before sending the next,
    // other output order is non-deterministic

    sender.sendMessage(encode("The"));
    waitForMessages();
    sender.sendMessage(encode("cat"));
    waitForMessages();
    sender.sendMessage(encode("sat"));
    waitForMessages();
    sender.sendMessage(encode("on"));
    waitForMessages();
    sender.sendMessage(encode("the"));
    waitForMessages();
    sender.sendMessage(encode("mat"));
    waitForMessages();

    checkTopics(listener, "a", 18);
    checkMessages(listener, "The", "The", "The", "cat", "cat", "cat", "sat", "sat", "sat", "on", "on", "on", "the", "the", "the", "mat", "mat", "mat");
  }

  @Test
  public void manyReceiversDifferentListenersSameTopic() throws Exception {
    final MessageSender sender = newSender("a");
    final StoringListener listener1 = newReceiver("a");
    final StoringListener listener2 = newReceiver("a");
    final StoringListener listener3 = newReceiver("a");

    sender.sendMessage(encode("The"));
    sender.sendMessage(encode("cat"));
    sender.sendMessage(encode("sat"));
    sender.sendMessage(encode("on"));
    sender.sendMessage(encode("the"));
    sender.sendMessage(encode("mat"));

    waitForMessages();

    // each receiver/listener gets each message
    checkTopics(listener1, "a", 6);
    checkTopics(listener2, "a", 6);
    checkTopics(listener3, "a", 6);
    checkMessages(listener1, "The", "cat", "sat", "on", "the", "mat");
    checkMessages(listener2, "The", "cat", "sat", "on", "the", "mat");
    checkMessages(listener3, "The", "cat", "sat", "on", "the", "mat");
  }

  @Test
  public void manyReceiversSameListenerDifferentTopics() throws Exception {
    final MessageSender sender = newSender("a");
    final StoringListener listener = new StoringListener();
    newReceiver("a", listener);
    newReceiver("b", listener);
    newReceiver("c", listener);

    sender.sendMessage(encode("The"));
    sender.sendMessage(encode("cat"));
    sender.sendMessage(encode("sat"));
    sender.sendMessage(encode("on"));
    sender.sendMessage(encode("the"));
    sender.sendMessage(encode("mat"));

    waitForMessages();

    // only receiver on topic "a" gets messages
    checkTopics(listener, "a", 6);
    checkMessages(listener, "The", "cat", "sat", "on", "the", "mat");
  }

  @Test
  public void manyReceiversDifferentListenersDifferentTopics() throws Exception {
    final MessageSender sender = newSender("a");
    final StoringListener listener1 = newReceiver("a");
    final StoringListener listener2 = newReceiver("b");
    final StoringListener listener3 = newReceiver("c");

    sender.sendMessage(encode("The"));
    sender.sendMessage(encode("cat"));
    sender.sendMessage(encode("sat"));
    sender.sendMessage(encode("on"));
    sender.sendMessage(encode("the"));
    sender.sendMessage(encode("mat"));

    waitForMessages();

    // only receiver/listener on topic "a" gets messages
    checkTopics(listener1, "a", 6);
    checkMessages(listener1, "The", "cat", "sat", "on", "the", "mat");
    assertTrue(listener2.messages.isEmpty());
    assertTrue(listener3.messages.isEmpty());
  }

  @Test
  public void manySendersAndReceivers() throws Exception {
    final MessageSender sender1 = newSender("a");
    final MessageSender sender2 = newSender("b");
    final MessageSender sender3 = newSender("b");
    final MessageSender sender4 = newSender("c");

    final StoringListener listener1 = new StoringListener();
    final StoringListener listener2 = new StoringListener();

    newReceiver("a", listener1);
    newReceiver("a", listener2);
    newReceiver("b", listener2);

    // listener1 receives from "a" via first receiver
    // listener2 receives from "a" via second receiver
    //                     and "b" via third receiver

    sender1.sendMessage(encode("Three-1-1"));
    waitForMessages();
    sender1.sendMessage(encode("blind-1-1"));
    waitForMessages();
    sender1.sendMessage(encode("mices-1-1"));
    waitForMessages();
    sender4.sendMessage(encode("Three-4-1"));
    waitForMessages();
    sender4.sendMessage(encode("blind-4-1"));
    waitForMessages();
    sender4.sendMessage(encode("mices-4-1"));
    waitForMessages();
    sender2.sendMessage(encode("Three-2-2"));
    waitForMessages();
    sender2.sendMessage(encode("blind-2-2"));
    waitForMessages();
    sender2.sendMessage(encode("mices-2-2"));
    waitForMessages();
    sender1.sendMessage(encode("Three-1-2"));
    waitForMessages();
    sender1.sendMessage(encode("blind-1-2"));
    waitForMessages();
    sender1.sendMessage(encode("mices-1-2"));
    waitForMessages();
    sender3.sendMessage(encode("Three-3-2"));
    waitForMessages();
    sender3.sendMessage(encode("blind-3-2"));
    waitForMessages();
    sender3.sendMessage(encode("mices-3-2"));
    waitForMessages();
    sender1.sendMessage(encode("Three-1-3"));
    waitForMessages();
    sender1.sendMessage(encode("blind-1-3"));
    waitForMessages();
    sender1.sendMessage(encode("mices-1-3"));
    waitForMessages();
    sender3.sendMessage(encode("Three-3-3"));
    waitForMessages();
    sender3.sendMessage(encode("blind-3-3"));
    waitForMessages();
    sender3.sendMessage(encode("mices-3-3"));
    waitForMessages();

    checkTopics(listener1, "a", 9);
    checkMessages(listener1,
        "Three-1-1", "blind-1-1", "mices-1-1",
        "Three-1-2", "blind-1-2", "mices-1-2",
        "Three-1-3", "blind-1-3", "mices-1-3");

    checkTopics(listener2,
        "a", "a", "a",
        "b", "b", "b",
        "a", "a", "a",
        "b", "b", "b",
        "a", "a", "a",
        "b", "b", "b");
    checkMessages(listener2,
        "Three-1-1", "blind-1-1", "mices-1-1",
        "Three-2-2", "blind-2-2", "mices-2-2",
        "Three-1-2", "blind-1-2", "mices-1-2",
        "Three-3-2", "blind-3-2", "mices-3-2",
        "Three-1-3", "blind-1-3", "mices-1-3",
        "Three-3-3", "blind-3-3", "mices-3-3");
  }

  @Test
  public void manySendersAndReceiversAcrossDifferentThreads() throws Exception {
    final MessageSender sender1 = newSender("a");
    final MessageSender sender2 = newSender("b");
    final MessageSender sender3 = newSender("b");
    final MessageSender sender4 = newSender("c");

    final StoringListener listener1 = new StoringListener();
    final StoringListener listener2 = new StoringListener();

    newReceiver("a", listener1);
    newReceiver("a", listener2);
    newReceiver("b", listener2);

    // listener1 receives from "a" via first receiver
    // listener2 receives from "a" via second receiver
    //                     and "b" via third receiver

    final Thread thread1 = new Thread(() -> {
      try {
        sender1.sendMessage(encode("Three-1-1"));
        sender1.sendMessage(encode("blind-1-1"));
        sender1.sendMessage(encode("mices-1-1"));

        sender4.sendMessage(encode("Three-4-1"));
        sender4.sendMessage(encode("blind-4-1"));
        sender4.sendMessage(encode("mices-4-1"));
      } catch (final MessagingException e) {
        // oops, have to wait for the test to fail...
      }
    });

    final Thread thread2 = new Thread(() -> {
      try {
        sender2.sendMessage(encode("Three-2-2"));
        sender2.sendMessage(encode("blind-2-2"));
        sender2.sendMessage(encode("mices-2-2"));

        sender1.sendMessage(encode("Three-1-2"));
        sender1.sendMessage(encode("blind-1-2"));
        sender1.sendMessage(encode("mices-1-2"));

        sender3.sendMessage(encode("Three-3-2"));
        sender3.sendMessage(encode("blind-3-2"));
        sender3.sendMessage(encode("mices-3-2"));
      } catch (final MessagingException e) {
        // oops, have to wait for the test to fail...
      }
    });

    final Thread thread3 = new Thread(() -> {
      try {
        sender1.sendMessage(encode("Three-1-3"));
        sender1.sendMessage(encode("blind-1-3"));
        sender1.sendMessage(encode("mices-1-3"));

        sender3.sendMessage(encode("Three-3-3"));
        sender3.sendMessage(encode("blind-3-3"));
        sender3.sendMessage(encode("mices-3-3"));
      } catch (final MessagingException e) {
        // oops, have to wait for the test to fail...
      }
    });

    thread1.start();
    thread2.start();
    thread3.start();

    thread1.join();
    thread2.join();
    thread3.join();

    waitForMessages();

    // Message ordering isn't deterministic with multiple producers, so
    // just check the right messages are received regardless of order

    checkTopics(listener1, "a", 9);

    assertEquals(Arrays.asList("Three-1-1", "Three-1-2", "Three-1-3",
        "blind-1-1", "blind-1-2", "blind-1-3",
        "mices-1-1", "mices-1-2", "mices-1-3"),
        listener1.messages.stream().map(MemoryMessagingTest::decode).sorted().collect(Collectors.toList()));

    assertEquals(Arrays.asList("a", "a", "a", "a", "a", "a", "a", "a", "a",  // count "xxxxx-1-x"
        "b", "b", "b", "b", "b", "b", "b", "b", "b"), // count "xxxxx-2-x" and "xxxxx-3-x"
        listener2.topics.stream().sorted().collect(Collectors.toList()));

    assertEquals(Arrays.asList("Three-1-1", "Three-1-2", "Three-1-3", "Three-2-2", "Three-3-2", "Three-3-3",
        "blind-1-1", "blind-1-2", "blind-1-3", "blind-2-2", "blind-3-2", "blind-3-3",
        "mices-1-1", "mices-1-2", "mices-1-3", "mices-2-2", "mices-3-2", "mices-3-3"),
        listener2.messages.stream().map(MemoryMessagingTest::decode).sorted().collect(Collectors.toList()));
  }

  @Test
  public void messageIsolation() throws Exception {
    final MessageSender sender = newSender("a");
    final StoringListener listener1 = new StoringListener();
    final StoringListener listener2 = new StoringListener();
    newReceiver("a", listener1);
    newReceiver("a", listener2);

    sender.sendMessage(encode("Hello"));
    waitForMessages();
    checkMessages(listener1, "Hello");
    checkMessages(listener2, "Hello");
    assertNotSame(listener1.messages.get(0), listener2.messages.get(0));
  }
}
