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

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class MCBaseScheme is the base {@link Scheme} to be used with the {@link MCSinkTap} for storing
 * key value pairs on a remote Memcached cluster.
 */
public abstract class MCBaseScheme<Config, Int, Value> extends Scheme<Config, Void, MCSchemeCollector, Object[], Void>
  {
  public MCBaseScheme( Fields sinkFields )
    {
    setSinkFields( sinkFields );
    }

  @Override
  public boolean isSource()
    {
    return false;
    }

  @Override
  public void sourceConfInit( FlowProcess<Config> flowProcess, Tap<Config, Void, MCSchemeCollector> output, Config conf )
    {
    }

  @Override
  public void sinkConfInit( FlowProcess<Config> flowProcess, Tap<Config, Void, MCSchemeCollector> output, Config conf )
    {
    }

  @Override
  public boolean source( FlowProcess<Config> flowProcess, SourceCall<Object[], Void> voidSourceCall ) throws IOException
    {
    throw new IllegalStateException( "source should never be called" );
    }

  protected abstract Int getIntermediate( TupleEntry tupleEntry );

  protected abstract String getKey( Int intermediate );

  protected abstract Value getValue( Int intermediate );

  @Override
  public void sink( FlowProcess<Config> flowProcess, SinkCall<Void, MCSchemeCollector> sinkCall ) throws IOException
    {
    Int intermediate = getIntermediate( sinkCall.getOutgoingEntry() );
    String key = getKey( intermediate );
    Value value = getValue( intermediate );

    sinkCall.getOutput().collect( key, value );
    }
  }
