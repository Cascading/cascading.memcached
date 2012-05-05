/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
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
  String hostnames = null;
  boolean useBinaryProtocol = true;
  int shutdownTimeoutSec = 5;
  int flushThreshold = 1000;

  public MCSinkTap( String hostnames, MCBaseScheme scheme )
    {
    super( scheme );
    this.hostnames = hostnames;
    }

  public MCSinkTap( String hostnames, MCBaseScheme scheme, boolean useBinaryProtocol )
    {
    this( hostnames, scheme, useBinaryProtocol, 5 );
    }

  public MCSinkTap( String hostnames, MCBaseScheme scheme, boolean useBinaryProtocol, int shutdownTimeoutSec )
    {
    this( hostnames, scheme, useBinaryProtocol, shutdownTimeoutSec, 1000 );
    }

  public MCSinkTap( String hostnames, MCBaseScheme scheme, boolean useBinaryProtocol, int shutdownTimeoutSec, int flushThreshold )
    {
    super( scheme, SinkMode.UPDATE );
    this.hostnames = hostnames;
    this.useBinaryProtocol = useBinaryProtocol;
    this.shutdownTimeoutSec = shutdownTimeoutSec;
    this.flushThreshold = flushThreshold;
    }

  @Override
  public TupleEntryCollector openForWrite( FlowProcess<Config> flowProcess, Object output ) throws IOException
    {
    return new MCSchemeCollector( flowProcess, (MCBaseScheme) getScheme(), hostnames, useBinaryProtocol, shutdownTimeoutSec );
    }

  @Override
  public String getIdentifier()
    {
    return "memcached/" + hostnames.replaceAll( ",|:", "_" );
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
