/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
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
