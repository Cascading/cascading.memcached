/*
 * Copyright (c) 2007-2013 Concurrent, Inc. All Rights Reserved.
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
import java.util.LinkedList;
import java.util.ListIterator;
import java.util.concurrent.TimeUnit;

import cascading.flow.FlowProcess;
import cascading.tap.TapException;
import cascading.tuple.TupleEntrySchemeCollector;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.ConnectionFactoryBuilder;
import net.spy.memcached.MemcachedClient;
import net.spy.memcached.internal.OperationFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 */
public class MCSchemeCollector<Config, Value> extends TupleEntrySchemeCollector<Config, TupleEntrySchemeCollector>
  {
  private static final Logger LOG = LoggerFactory.getLogger( MCSchemeCollector.class );

  private MemcachedClient client;
  private int shutdownTimeoutSec = 5;
  private int flushThreshold = 1000;

  private LinkedList<OperationFuture<Boolean>> futures = new LinkedList<OperationFuture<Boolean>>();

  MCSchemeCollector( FlowProcess<Config> flowProcess, MCBaseScheme scheme, String hostnames, boolean useBinary, int shutdownTimeoutSec ) throws IOException
    {
    this( flowProcess, scheme, hostnames, useBinary, shutdownTimeoutSec, 1000 );
    }

  MCSchemeCollector( FlowProcess<Config> flowProcess, MCBaseScheme scheme, String hostnames, boolean useBinary, int shutdownTimeoutSec, int flushThreshold ) throws IOException
    {
    super( flowProcess, scheme );
    this.shutdownTimeoutSec = shutdownTimeoutSec;
    this.flushThreshold = flushThreshold;
    ConnectionFactoryBuilder builder = new ConnectionFactoryBuilder();

    ConnectionFactoryBuilder.Protocol protocol = useBinary ? ConnectionFactoryBuilder.Protocol.BINARY : ConnectionFactoryBuilder.Protocol.TEXT;

    builder = builder.setProtocol( protocol ).setOpQueueMaxBlockTime( 1000 );

    client = new MemcachedClient( builder.build(), AddrUtil.getAddresses( hostnames ) );

    setOutput( this );
    }

  public void collect( String key, Value value ) throws IOException
    {
    OperationFuture<Boolean> future = retry( key, value, 2000, 3 );

    if( future == null )
      throw new TapException( "unable to store value" );

    futures.add( future );

    if( futures.size() >= flushThreshold )
      fullFlush();
    }

  private OperationFuture<Boolean> retry( String key, Object value, int duration, int tries )
    {
    if( tries == 0 )
      return null;

    try
      {
      return client.set( key, 0, value );
      }
    catch( IllegalStateException exception )
      {
      LOG.warn( "retrying set operation" );
      sleepSafe( duration );
      return retry( key, value, duration * 2, tries - 1 );
      }
    }

  private void sleepSafe( int duration )
    {
    try
      {
      Thread.sleep( duration );
      }
    catch( InterruptedException exception )
      {
      // do nothing
      }
    }

  private void fullFlush()
    {
    while( !futures.isEmpty() )
      flush();
    }

  private void flush()
    {
    ListIterator<OperationFuture<Boolean>> iterator = futures.listIterator();

    while( iterator.hasNext() )
      {
      OperationFuture<Boolean> future = iterator.next();

      if( future.isCancelled() )
        throw new TapException( "operation was canceled: " + future.getStatus().getMessage() );

      if( future.isDone() )
        iterator.remove();
      }
    }

  @Override
  public void close()
    {
    try
      {
      fullFlush();
      }
    finally
      {
      client.shutdown( shutdownTimeoutSec, TimeUnit.SECONDS );
      }
    }
  }
