/*
 * Copyright (c) 2013 Ramon Servadei 
 *  
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *    
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.fimtra.tcpchannel;

import com.fimtra.tcpchannel.TcpChannel.FrameEncodingFormatEnum;

/**
 * Defines the properties and property keys used by TcpChannel
 * 
 * @author Ramon Servadei
 */
public abstract class TcpChannelProperties
{
    private static final String BASE = "tcpChannel.";
    /**
     * The system property name to define the receive buffer size in bytes.<br>
     * E.g. <code>-DtcpChannel.rxBufferSize=65535</code>
     */
    public static final String PROPERTY_NAME_RX_BUFFER_SIZE = BASE + "rxBufferSize";
    /**
     * The system property name to define the send buffer size in bytes. This MUST always be less
     * than the receive buffer size.<br>
     * E.g. <code>-DtcpChannel.txBufferSize=1024</code>
     */
    public static final String PROPERTY_NAME_TX_BUFFER_SIZE = BASE + "txBufferSize";
    /**
     * The system property name to define the frame encoding. Value is one of the
     * {@link FrameEncodingFormatEnum}s<br>
     * E.g. <code>-DtcpChannel.frameEncoding=TERMINATOR_BASED</code>
     */
    public static final String PROPERTY_NAME_FRAME_ENCODING = BASE + "frameEncoding";

    /** The frame encoding, default is TERMINATOR_BASED */
    public static final TcpChannel.FrameEncodingFormatEnum FRAME_ENCODING =
        TcpChannel.FrameEncodingFormatEnum.valueOf(System.getProperty(PROPERTY_NAME_FRAME_ENCODING,
            TcpChannel.FrameEncodingFormatEnum.TERMINATOR_BASED.toString()));

    /** The receive buffer size, default is 65k */
    public static final int RX_BUFFER_SIZE =
        Integer.parseInt(System.getProperty(PROPERTY_NAME_RX_BUFFER_SIZE, "65535"));

    /** The send buffer size, default is 1k */
    public static final int TX_SEND_SIZE = Integer.parseInt(System.getProperty(PROPERTY_NAME_TX_BUFFER_SIZE, "1024"));

    private TcpChannelProperties()
    {
    }
}