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

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 *
 */
public abstract class MCTupleEntryScheme<Config, Value> extends MCBaseScheme<Config, TupleEntry, Value>
  {
  private String keyDelim = ":";
  private Fields keyFields;
  private Fields valueFields;

  public MCTupleEntryScheme( Fields keyFields, Fields valueFields )
    {
    this( keyFields, valueFields, ":" );
    }

  public MCTupleEntryScheme( Fields keyFields, Fields valueFields, String keyDelim )
    {
    super( Fields.merge( keyFields, valueFields ) );
    this.keyFields = keyFields;
    this.valueFields = valueFields;
    this.keyDelim = keyDelim;
    }

  public String getKeyDelim()
    {
    return keyDelim;
    }

  public Fields getKeyFields()
    {
    return keyFields;
    }

  public Fields getValueFields()
    {
    return valueFields;
    }

  @Override
  protected TupleEntry getIntermediate( TupleEntry tupleEntry )
    {
    return tupleEntry;
    }

  @Override
  protected String getKey( TupleEntry tupleEntry )
    {
    return tupleEntry.selectTuple( getKeyFields() ).toString( getKeyDelim(), false );
    }

  protected abstract Value getValue( TupleEntry tupleEntry );
  }
