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

import cascading.tuple.Fields;
import cascading.tuple.TupleEntry;

/**
 * Class MCDelimitedScheme will create a string from the values selected by the valueFields delimited by
 * the given valueDelim (TAB is default).
 */
public class MCDelimitedScheme<Config> extends MCTupleEntryScheme<Config, String>
  {
  public static final String DELIMITER = "\t";

  String valueDelimiter = DELIMITER;

  public MCDelimitedScheme( Fields keyFields, Fields valueFields )
    {
    super( keyFields, valueFields );
    }

  public MCDelimitedScheme( Fields keyFields, Fields valueFields, boolean cleanKey, String keyDelimiter, String valueDelimiter )
    {
    super( keyFields, valueFields, cleanKey, keyDelimiter );
    this.valueDelimiter = valueDelimiter;
    }

  public MCDelimitedScheme( Fields keyFields, Fields valueFields, String valueDelimiter )
    {
    super( keyFields, valueFields );
    this.valueDelimiter = valueDelimiter;
    }

  @Override
  protected String getValue( TupleEntry tupleEntry )
    {
    return tupleEntry.selectTuple( getValueFields() ).toString( valueDelimiter, false );
    }
  }
