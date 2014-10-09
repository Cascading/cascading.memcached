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

import java.util.Properties;

import cascading.scheme.Scheme;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static cascading.memcached.MCDelimitedScheme.DELIMITER;
import static cascading.memcached.MCSinkTap.*;
import static cascading.memcached.MCTupleEntryScheme.CLEAN_KEY_DEFAULT;
import static cascading.memcached.MCTupleEntryScheme.KEY_DELIMITER;

/**
 *
 */
public class MCFactory
  {
  private static final Logger LOG = LoggerFactory.getLogger( MCFactory.class );

  public static final String PROTOCOL_USE_BINARY_PROTOCOL = "useBinaryProtocol";
  public static final String PROTOCOL_SHUTDOWN_TIMEOUT_SEC = "shutdownTimeoutSec";
  public static final String PROTOCOL_FLUSH_THRESHOLD = "flushThreshold";

  public static final String FORMAT_KEY_FIELDS = "keyFields";
  public static final String FORMAT_VALUE_FIELDS = "valueFields";
  public static final String FORMAT_CLEAN_KEY = "cleanKey";
  public static final String FORMAT_KEY_DELIMITER = "keyDelimiter";
  public static final String FORMAT_VALUE_DELIMITER = "valueDelimiter";

  public Tap createTap( Scheme scheme, String path, SinkMode sinkMode, Properties properties )
    {
    boolean useBinaryProtocol = Boolean.parseBoolean( properties.getProperty( PROTOCOL_USE_BINARY_PROTOCOL, Boolean.toString( USE_BINARY ) ) );
    int shutdownTimeoutSec = Integer.parseInt( properties.getProperty( PROTOCOL_SHUTDOWN_TIMEOUT_SEC, Integer.toString( SHUTDOWN_TIMEOUT_SEC ) ) );
    int flushThreshold = Integer.parseInt( properties.getProperty( PROTOCOL_FLUSH_THRESHOLD, Integer.toString( FLUSH_THRESHOLD ) ) );

    LOG.info( "creating memcached protocol" );

    return new MCSinkTap( path, (MCBaseScheme) scheme, useBinaryProtocol, shutdownTimeoutSec, flushThreshold );
    }

  public Scheme createScheme( Fields fields, Properties properties )
    {
    String keyFields = properties.getProperty( FORMAT_KEY_FIELDS );
    String valueFields = properties.getProperty( FORMAT_VALUE_FIELDS );

    if( keyFields == null )
      throw new IllegalArgumentException( "keyFields not given" );

    if( valueFields == null )
      throw new IllegalArgumentException( "valueFields not given" );

    boolean cleanKey = Boolean.parseBoolean( properties.getProperty( FORMAT_CLEAN_KEY, Boolean.toString( CLEAN_KEY_DEFAULT ) ) );
    String keyDelimiter = properties.getProperty( FORMAT_KEY_DELIMITER, KEY_DELIMITER );
    String valueDelimiter = properties.getProperty( FORMAT_VALUE_DELIMITER, DELIMITER );

    LOG.info( "creating memcached format with keys: {}, values: {}, delimiter: {}", keyFields, valueFields, valueDelimiter );

    Fields keys = new Fields( keyFields.split( "," ) );
    Fields values = new Fields( valueFields.split( "," ) );

    keys = fields.select( keys );
    values = fields.select( values );

    LOG.info( "using actual fields keys: {}, values: {}", keys.printVerbose(), values.printVerbose() );

    return new MCDelimitedScheme( keys, values, cleanKey, keyDelimiter, valueDelimiter );
    }
  }
