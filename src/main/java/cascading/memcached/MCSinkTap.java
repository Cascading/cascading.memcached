/*
 * Copyright (c) 2007-2015 Concurrent, Inc. All Rights Reserved.
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

import cascading.flow.FlowProcess;
import cascading.tap.SinkMode;
import cascading.tap.SinkTap;
import cascading.tuple.TupleEntryCollector;

/**
 * Class MCSinkTap is a {@link cascading.tap.Tap} class only support sinking data to a Memcached cluster (or cluster supporting
 * the text and binary protocols).
 * <p/>
 * This Tap must be used with a {@link MCBaseScheme} sub-class.
 *
 * @see cascading.memcached.MCTupleScheme
 * @see cascading.memcached.MCTupleEntryScheme
 * @see cascading.memcached.MCDelimitedScheme
 */
public class MCSinkTap<Config> extends SinkTap<Config, Object>
  {
  public static final boolean USE_BINARY = true;
  public static final int SHUTDOWN_TIMEOUT_SEC = 5;
  public static final int FLUSH_THRESHOLD = 1000;

  String hostNames = null;
  boolean useBinaryProtocol = USE_BINARY;
  int shutdownTimeoutSec = SHUTDOWN_TIMEOUT_SEC;
  int flushThreshold = FLUSH_THRESHOLD;

  public MCSinkTap( String hostNames, MCBaseScheme scheme )
    {
    super( scheme );
    this.hostNames = hostNames;
    }

  public MCSinkTap( String hostNames, MCBaseScheme scheme, boolean useBinaryProtocol )
    {
    this( hostNames, scheme, useBinaryProtocol, 5 );
    }

  public MCSinkTap( String hostNames, MCBaseScheme scheme, boolean useBinaryProtocol, int shutdownTimeoutSec )
    {
    this( hostNames, scheme, useBinaryProtocol, shutdownTimeoutSec, 1000 );
    }

  public MCSinkTap( String hostNames, MCBaseScheme scheme, boolean useBinaryProtocol, int shutdownTimeoutSec, int flushThreshold )
    {
    super( scheme, SinkMode.UPDATE );
    this.hostNames = hostNames;
    this.useBinaryProtocol = useBinaryProtocol;
    this.shutdownTimeoutSec = shutdownTimeoutSec;
    this.flushThreshold = flushThreshold;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<Config> flowProcess, Object output ) throws IOException
    {
    return new MCSchemeCollector( flowProcess, (MCBaseScheme) getScheme(), hostNames, useBinaryProtocol, shutdownTimeoutSec, flushThreshold );
    }

  @Override
  public String getIdentifier()
    {
    if( useBinaryProtocol )
      return "memcache://" + hostNames.replaceAll( ",|:", "_" );
    else
      return "memcachet://" + hostNames.replaceAll( ",|:", "_" ); // t for text
    }

  @Override
  public boolean createResource( Config conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean deleteResource( Config conf ) throws IOException
    {
    return true;
    }

  @Override
  public boolean resourceExists( Config conf ) throws IOException
    {
    return true;
    }

  @Override
  public long getModifiedTime( Config conf ) throws IOException
    {
    return 0; // always stale
    }
  }
