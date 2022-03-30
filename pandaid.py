import inspect

def select(df, where=None, **args):
    '''Return a dataframe with a subset of this one's rows and cols.

    Arguments
    =========

    * df:
        The source dataframe.

    * where:
        A string with a boolean clause, such as 'mycol > 123'. You can pass variables from the
        environment by prefixing with '@'. If your column name has a space, wrap it in backticks.
        See expr argument in pandas.DataFrame.query or .eval for more.

    * col_name, row_name:
        Usually a str, or slice of str, or array of str. But if your dataframe has other kinds of
        names (like integers) you can pass those here too.
        Pass a slice or array to select multiple values.

    * col_number, row_number:
        An int, or slice of int, or array of int.
        Pass a slice or array to select multiple values.

    Returns:
        A Pandas DataFrame with the result.

    Example
    =======

    >>> select(
    >>>   mydf,
    >>>   where='`stock ticker` in ["AAPL", "GOOG"]',
    >>>   col_name=['stock ticker', 'country'])

    Hints:
    1. Call value(df) to get a scalar value for the top-left cell.
    2. Use df.groupby('column_name').operation() to aggregage (where opeartion is mean(), max(), etc)
    '''

    if where:
        df = df.query(where, local_dict=inspect.currentframe().f_back.f_locals)

    if args:
        return _subset(df, **args)

    return df

def value(
    df,
    row_name=None,
    col_name=None,
    row_number=None,
    col_number=None):
    "Get the value of a dataframe cell. Arguments are self-explanatory!"

    if col_name is not None and row_name is not None :
        return df.at[row_name, col_name]

    if col_number is not None and row_number is not None :
        return df.iat[row_number, col_number]

    if col_number is None and row_number is None:
        return df.iat[0, 0]

    raise ValueError("You can't mix *_name and *_number arguments")

def _to_seq(x):
    if isinstance(x, (list, tuple, slice)):
        return x
    elif x is None:
        return []
    else:
        return [x]

def _subset(
    df,
    row_name=None,
    col_name=None,
    row_number=None,
    col_number=None):

    if row_name and row_number:
        raise ValueError("You can't pass both row_name and row_number")

    if col_name and col_number:
        raise ValueError("You can't pass both col_name and col_number")

    row_name = _to_seq(row_name)
    row_number = _to_seq(row_number)
    col_name = _to_seq(col_name)
    col_number = _to_seq(col_number)

    if not row_name and not row_number:
        row_name = slice(None)

    if not col_name and not col_number:
        col_name = slice(None)

    if row_name:

        if col_name:
            return df.loc[row_name, col_name]
        else:
            return df.loc[row_name, slice(None)].iloc[slice(None), col_number]
    else:
        if col_name:
            return df.loc[slice(None), col_name].iloc[row_number, slice(None)]
        else:
            return df.iloc[row_number, col_number]
