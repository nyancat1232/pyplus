
def write_columns(*positional_data,**keyword_data):
    '''
    write by columns
    ## Parameters:
    positional_data : Any
        ...
    keyword_data : Any
        ....
    ## See Also:
    st.write
    st.columns
    ## Examples:
    >>> from pyplus.streamlit.streamlit_plus import write_columns
    >>> import streamlit as st
    >>> xx=np.array([['x1','x2'],['x3','x4']])
    >>> ww=np.array([['w1','w2'],['w3','w4']])
    >>> write_columns(X1=xx,W1=ww)
    X1           W1
    0    1       0       1
    x1   x2      w1      w2
    x3   x4      w3      w4

    '''
    if len(positional_data)+len(keyword_data)<1:
        st.write('No arguments')
        return

    column_table = st.columns(len(positional_data)+len(keyword_data))

    total_data = dict(enumerate(positional_data))
    total_data.update(keyword_data)

    for index,key in enumerate(total_data):
        with column_table[index]:
            st.write(key)
            st.write(total_data[key])

