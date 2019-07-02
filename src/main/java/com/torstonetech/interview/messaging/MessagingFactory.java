package com.torstonetech.interview.messaging;

/**
 * Provides bootstrap and shutdown operations for a messaging provider within an application and acts
 * as a factory for {@code MessageSender} and {@code MessageReceiver} instances.
 */
public interface MessagingFactory {
  /**
   * Stop any underlying resources in advance of the application shutting down.
   * <p/>
   * This is typically only ever called once at application shutdown.
   *
   * @throws Exception If the messaging infrastructure cannot be shutdown. There isn't much that
   *                   can be done by the caller in this situation other than to log the problem.
   */
  void shutdown() throws Exception;

  /**
   * @return A human readable name for the messaging provider.
   */
  String getProviderName();

  /**
   * Creates a message sender on the given topic.
   *
   * @param topic The topic.
   * @return A new message sender.
   * @throws MessagingException If the factory is unable to create a sender.
   */
  MessageSender createSender(String topic) throws MessagingException;

  /**
   * Create a message receiver on the given topic.
   *
   * @param topic The topic.
   * @return A new message receiver.
   * @throws MessagingException If the factory is unable to create a receiver.
   */
  MessageReceiver createReceiver(String topic) throws MessagingException;
}
