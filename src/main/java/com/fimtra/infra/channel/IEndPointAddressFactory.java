/*
 * Copyright (c) 2014 Ramon Servadei 
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
package com.fimtra.infra.channel;

/**
 * Encapsulates providing {@link EndPointAddress} instances. This enables redundancy of addresses to
 * be achieved - each call to {@link #next()} can return the next address to try.
 * 
 * @author Ramon Servadei
 */
public interface IEndPointAddressFactory
{
    /**
     * Call to retrieve the 'next' address to try to connect to
     * 
     * @return an end-point address to connect to
     */
    EndPointAddress next();
}
