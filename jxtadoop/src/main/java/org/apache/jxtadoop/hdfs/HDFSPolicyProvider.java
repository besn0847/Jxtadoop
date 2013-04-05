/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.jxtadoop.hdfs;

import org.apache.jxtadoop.hdfs.protocol.ClientDatanodeProtocol;
import org.apache.jxtadoop.hdfs.protocol.ClientProtocol;
import org.apache.jxtadoop.hdfs.server.protocol.DatanodeProtocol;
import org.apache.jxtadoop.hdfs.server.protocol.InterDatanodeProtocol;
import org.apache.jxtadoop.hdfs.server.protocol.NamenodeProtocol;
import org.apache.jxtadoop.security.authorize.PolicyProvider;
import org.apache.jxtadoop.security.authorize.RefreshAuthorizationPolicyProtocol;
import org.apache.jxtadoop.security.authorize.Service;

/**
 * {@link PolicyProvider} for HDFS protocols.
 */
public class HDFSPolicyProvider extends PolicyProvider {
	
  private static final Service[] hdfsServices =
    new Service[] {
    new Service("security.client.protocol.acl", ClientProtocol.class),
    new Service("security.client.datanode.protocol.acl", 
                ClientDatanodeProtocol.class),
    new Service("security.datanode.protocol.acl", DatanodeProtocol.class),
    new Service("security.inter.datanode.protocol.acl", 
                InterDatanodeProtocol.class),
    new Service("security.namenode.protocol.acl", NamenodeProtocol.class),
    new Service("security.refresh.policy.protocol.acl", 
                RefreshAuthorizationPolicyProtocol.class),
  };
  
  @Override
  public Service[] getServices() {
    return hdfsServices;
  }
}
