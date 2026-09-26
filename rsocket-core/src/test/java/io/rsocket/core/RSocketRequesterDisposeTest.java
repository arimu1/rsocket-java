/*
 * Copyright 2015-2026 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.rsocket.core;

import static io.rsocket.frame.FrameLengthCodec.FRAME_LENGTH_MASK;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.rsocket.Payload;
import io.rsocket.RSocketErrorException;
import io.rsocket.buffer.LeaksTrackingByteBufAllocator;
import io.rsocket.exceptions.ConnectionErrorException;
import io.rsocket.frame.ErrorFrameCodec;
import io.rsocket.frame.FrameHeaderCodec;
import io.rsocket.frame.FrameType;
import io.rsocket.frame.decoder.PayloadDecoder;
import io.rsocket.test.util.TestDuplexConnection;
import io.rsocket.util.EmptyPayload;
import java.time.Duration;
import io.netty.util.ReferenceCountUtil;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Sinks;
import reactor.test.StepVerifier;

/** Regression tests for https://github.com/rsocket/rsocket-java/issues/1126 */
class RSocketRequesterDisposeTest {

  private final LeaksTrackingByteBufAllocator allocator =
      LeaksTrackingByteBufAllocator.instrument(ByteBufAllocator.DEFAULT);

  private NettyLikeDuplexConnection connection;
  private Sinks.Empty<Void> requesterClosedSink;
  private RSocketRequester requester;

  @AfterEach
  void tearDown() {
    if (connection != null) {
      connection.getSent().forEach(ReferenceCountUtil::safeRelease);
      connection.clearSendReceiveBuffers();
      allocator.assertHasNoLeaks();
    }
  }

  @Test
  void disposeTerminatesRequesterWhenSendErrorAndCloseDoesNotCloseConnection() {
    connection = new NettyLikeDuplexConnection(allocator);
    requesterClosedSink = Sinks.empty();
    requester =
        new RSocketRequester(
            connection,
            PayloadDecoder.ZERO_COPY,
            StreamIdSupplier.clientSupplier(),
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            0,
            0,
            null,
            __ -> null,
            null,
            requesterClosedSink,
            requesterClosedSink.asMono());

    StepVerifier.create(requester.onClose())
        .then(requester::dispose)
        .expectError(ConnectionErrorException.class)
        .verify(Duration.ofSeconds(5));

    Assertions.assertThat(requester.isDisposed()).isTrue();
    Assertions.assertThat(connection.isDisposed()).isTrue();

    Payload payload = EmptyPayload.INSTANCE;
    StepVerifier.create(requester.requestResponse(payload))
        .expectError(ConnectionErrorException.class)
        .verify(Duration.ofSeconds(5));
  }

  @Test
  void disposeSendsConnectionErrorBeforeClosing() {
    connection = new NettyLikeDuplexConnection(allocator);
    requesterClosedSink = Sinks.empty();
    requester =
        new RSocketRequester(
            connection,
            PayloadDecoder.ZERO_COPY,
            StreamIdSupplier.clientSupplier(),
            0,
            FRAME_LENGTH_MASK,
            Integer.MAX_VALUE,
            0,
            0,
            null,
            __ -> null,
            null,
            requesterClosedSink,
            requesterClosedSink.asMono());

    requester.dispose();

    Assertions.assertThat(connection.getSent())
        .hasSize(1)
        .first()
        .satisfies(
            frame -> {
              Assertions.assertThat(FrameHeaderCodec.frameType(frame)).isEqualTo(FrameType.ERROR);
              frame.release();
            });
  }

  /**
   * Mirrors {@link io.rsocket.transport.netty.TcpDuplexConnection#sendErrorAndClose}: sends a
   * connection error frame without completing {@link io.rsocket.DuplexConnection#onClose()}.
   */
  static final class NettyLikeDuplexConnection extends TestDuplexConnection {

    NettyLikeDuplexConnection(LeaksTrackingByteBufAllocator allocator) {
      super(allocator);
    }

    @Override
    public void sendErrorAndClose(RSocketErrorException e) {
      ByteBuf errorFrame = ErrorFrameCodec.encode(alloc(), 0, e);
      sendFrame(0, errorFrame);
    }
  }
}
