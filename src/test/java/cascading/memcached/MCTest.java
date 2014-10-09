/*
 * Copyright (c) 2007-2014 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.cascading.org/
 *
 * This file is part of the Cascading project.
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

package cascading.memcached;

import java.io.IOException;
import java.net.InetSocketAddress;

import cascading.PlatformTestCase;
import cascading.flow.Flow;
import cascading.pipe.Pipe;
import cascading.tap.Tap;
import cascading.test.HadoopPlatform;
import cascading.test.LocalPlatform;
import cascading.test.PlatformRunner;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import com.thimbleware.jmemcached.CacheImpl;
import com.thimbleware.jmemcached.Key;
import com.thimbleware.jmemcached.LocalCacheElement;
import com.thimbleware.jmemcached.MemCacheDaemon;
import com.thimbleware.jmemcached.storage.CacheStorage;
import com.thimbleware.jmemcached.storage.hash.ConcurrentLinkedHashMap;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import org.apache.log4j.Level;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 *
 */
@PlatformRunner.Platform({LocalPlatform.class, HadoopPlatform.class})
public class MCTest extends PlatformTestCase
  {
  String inputFile = "src/test/data/small.txt";
  private MemCacheDaemon<LocalCacheElement> daemon;
  private boolean useBinaryProtocol;

  public MCTest()
    {
    super( true, 1, 4 );
    }

  @Before
  @Override
  public void setUp() throws Exception
    {
    useBinaryProtocol = false;
    super.setUp();

    daemon = new MemCacheDaemon<LocalCacheElement>();

    CacheStorage<Key, LocalCacheElement> storage =
      ConcurrentLinkedHashMap.create( ConcurrentLinkedHashMap.EvictionPolicy.FIFO, Short.MAX_VALUE, Integer.MAX_VALUE );

    daemon.setCache( new CacheImpl( storage ) );
    daemon.setBinary( useBinaryProtocol );
    daemon.setAddr( new InetSocketAddress( "127.0.0.1", 11211 ) );
    daemon.setIdleTime( 100000 );
    daemon.setVerbose( true );

    daemon.start();

    org.apache.log4j.Logger.getLogger( "cascading" ).setLevel( Level.toLevel( "INFO" ) );
    org.apache.log4j.Logger.getLogger( "org.apache.hadoop" ).setLevel( Level.toLevel( "INFO" ) );
    }

  @After
  @Override
  public void tearDown() throws Exception
    {
    super.tearDown();

    daemon.stop();
    }

  private MemcachedClient getClient() throws IOException
    {
    ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();
    builder = builder.setProtocol( useBinaryProtocol ? ConnectionFactoryBuilder.Protocol.BINARY : ConnectionFactoryBuilder.Protocol.TEXT );
    builder = builder.setOpQueueMaxBlockTime( 1000 );

    return new MemcachedClient( builder.build(), AddrUtil.getAddresses( "localhost:11211" ) );
    }

//    1 c
//    2 d
//    3 c
//    4 d
//    5 e

  @Test
  public void testTupleScheme() throws IOException
    {
    runTupleTest( useBinaryProtocol );
    }

  private void runTupleTest( boolean useBinary ) throws IOException
    {
    runTestFor( new MCTupleScheme( new Fields( "num" ), new Fields( "lower" ) ), useBinary );

    MemcachedClient client = getClient();

    assertEquals( "c", ( (Tuple) client.get( "1" ) ).get( 0 ) );
    assertEquals( "d", ( (Tuple) client.get( "2" ) ).get( 0 ) );
    assertEquals( "c", ( (Tuple) client.get( "3" ) ).get( 0 ) );
    assertEquals( "d", ( (Tuple) client.get( "4" ) ).get( 0 ) );
    assertEquals( "e", ( (Tuple) client.get( "5" ) ).get( 0 ) );

    client.shutdown();
    }

  @Test
  public void testDelimitedScheme() throws IOException
    {
    runDelimitedTest( useBinaryProtocol );
    }

  private void runDelimitedTest( boolean useBinary ) throws IOException
    {
    runTestFor( new MCDelimitedScheme( new Fields( "num" ), new Fields( "lower" ) ), useBinary );

    MemcachedClient client = getClient();

    assertEquals( "c", client.get( "1" ) );
    assertEquals( "d", client.get( "2" ) );
    assertEquals( "c", client.get( "3" ) );
    assertEquals( "d", client.get( "4" ) );
    assertEquals( "e", client.get( "5" ) );

    client.shutdown();
    }

  private void runTestFor( MCBaseScheme scheme, boolean useBinary ) throws IOException
    {
    getPlatform().copyFromLocal( inputFile );

    Tap source = getPlatform().getDelimitedFile( new Fields( "num", "lower", "upper" ), " ", inputFile );

    Tap sink = new MCSinkTap( "localhost:11211", scheme, useBinary );

    Flow flow = getPlatform().getFlowConnector().connect( source, sink, new Pipe( "identity" ) );

    flow.complete();
    }
  }
