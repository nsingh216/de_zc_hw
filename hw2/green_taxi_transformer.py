if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """

    Args:
        data: The output from the upstream parent block (dataframe with green taxi data)
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        df2 (dataframe with transformed columns)
    """

    ## vendor ids values pre transform
    print(data['VendorID'].unique())    
        
    df2 = data[~((data['passenger_count'] == 0) | (data['trip_distance'] == 0))]

    # Create a new column lpep_pickup_date by converting lpep_pickup_datetime to a date.
    df2['lpep_pickup_date'] = df2['lpep_pickup_datetime'].dt.date

    print(df2.shape)

    # Rename columns in Camel Case to Snake Case
    df2.columns = convert_to_snake_case(df2.columns)

    ## vendor ids values post transform
    print(df2['vendor_id'].unique())


    print(df2.columns)
    print(df2.dtypes)

    return df2 



def convert_to_snake_case(cols):

    new_cols = []
    keep_together = ['ID', 'PU', 'DO']
        
    for col_name in cols:

        for root in keep_together:
            
            index = col_name.find(root)
                
            if index == 0:
                col_name = col_name[index: index+2].lower() + "_" + col_name[index+2: ]
                
            elif index > 0:
                col_name = col_name[0: index] + "_" + col_name[index: index+2].lower() + col_name[index+2:]
                
            else:
                col_name = col_name
                
        new_cols.append(col_name.lower())
   
    return new_cols



@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'

    # add 3 assertions

    assert 'vendor_id' in output.columns, "vendor_id exists"

    assert output[(output['passenger_count'] <= 0)].shape[0] == 0, "passenger count is always greater than 0"

    assert output[(output['trip_distance'] <= 0)].shape[0] == 0, "trip_distance is always greater than 0"


