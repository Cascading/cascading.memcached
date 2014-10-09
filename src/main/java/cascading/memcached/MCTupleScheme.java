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

import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

/**
 * Class MCTupleEntryScheme will create a key from the keyFields values and try to store a Tuple from the
 * valueFields values.
 */
public class MCTupleScheme<Config> extends MCTupleEntryScheme<Config, Tuple>
  {
  public MCTupleScheme( Fields keyFields, Fields valueFields )
    {
    this( keyFields, valueFields, ":" );
    }

  public MCTupleScheme( Fields keyFields, Fields valueFields, String keyDelim )
    {
    super( keyFields, valueFields, keyDelim );
    }

  protected Tuple getValue( TupleEntry tupleEntry )
    {
    return tupleEntry.selectTuple( getValueFields() );
    }
  }
