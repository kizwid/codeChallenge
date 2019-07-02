package com.torstonetech.interview.messaging;

public interface MessageReceiveListener {
  /**
   * Handle the given message.
   * <p/>
   * Implementations should call the message's {@link Message#dispose()} method once they are done with it.
   *
   * @param message Received message.
   * @param topic   Topic to receive the message from.
   */
  void onMessage(Message message, String topic);
}
