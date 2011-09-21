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
import cascading.util.Util;

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

  @Override
  public boolean equals( Object object )
    {
    if( this == object )
      return true;
    if( object == null || getClass() != object.getClass() )
      return false;
    if( !super.equals( object ) )
      return false;

    MCSinkTap mcSinkTap = (MCSinkTap) object;

    if( useBinaryProtocol != mcSinkTap.useBinaryProtocol )
      return false;
    if( hostnames != null ? !hostnames.equals( mcSinkTap.hostnames ) : mcSinkTap.hostnames != null )
      return false;

    return true;
    }

  @Override
  public int hashCode()
    {
    int result = super.hashCode();
    result = 31 * result + ( hostnames != null ? hostnames.hashCode() : 0 );
    result = 31 * result + ( useBinaryProtocol ? 1 : 0 );
    return result;
    }

  @Override
  public String toString()
    {
    if( hostnames != null )
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[\"" + Util.sanitizeUrl( hostnames ) + "\"]"; // sanitize
    else
      return getClass().getSimpleName() + "[\"" + getScheme() + "\"]" + "[not initialized]";
    }
  }
