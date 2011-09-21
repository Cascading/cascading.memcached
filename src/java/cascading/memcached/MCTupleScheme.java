/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
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
