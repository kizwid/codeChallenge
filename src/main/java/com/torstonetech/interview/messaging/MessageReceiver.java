package com.torstonetech.interview.messaging;

/**
 * Note that wildcard subscriptions are not currently supported.
 */
public interface MessageReceiver {
  /**
   * @return This receiver's topic.
   */
  String getTopic();

  /**
   * Set the listener to be called when a message arrives. Only one listener is allowed per receiver.
   * It is up to implementations whether they allow it to be re-assigned, although it is intended
   * this will be called only once.
   *
   * @param listener Listener for messages.
   */
  void setListener(MessageReceiveListener listener);
}
