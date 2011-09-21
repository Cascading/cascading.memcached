/*
 * Copyright (c) 2007-2012 Concurrent, Inc. All Rights Reserved.
 *
 * Project and contact information: http://www.concurrentinc.com/
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
  String valueDelim = "\t";

  public MCDelimitedScheme( Fields keyFields, Fields valueFields )
    {
    super( keyFields, valueFields );
    }

  public MCDelimitedScheme( Fields keyFields, Fields valueFields, String valueDelim )
    {
    super( keyFields, valueFields );
    this.valueDelim = valueDelim;
    }

  @Override
  protected String getValue( TupleEntry tupleEntry )
    {
    return tupleEntry.selectTuple( getValueFields() ).toString( valueDelim, false );
    }
  }
