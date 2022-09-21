# def func(agg, *args=[], **kwargs={}):
blog1 = 'blog1'
blog2 = 'blog2'
blog3 = 'blog3'
title = 'title1'

def blog_posts(title, *args):
    print(title)
    for post in args:
        print('OK '+post)
# blog_posts(title, blog1, blog2, blog3)

def blog_posts_kw(title, **kwargs):
    print(title)
    for kw, post in kwargs.items():
        print(kw +' OK '+post)


# blog_posts_kw(title, blog1 = 'blog1', blog2 = 'blog2', blog3 = 'blog3')

def blog_posts_arg_kw(title, *args, **kwargs):
    print(title)
    for arg in args:
        print(arg)
    for kw, post in kwargs.items():
        print(kw +' OK '+post)

# blog_posts_arg_kw(title, 1,2,3, blog1 = 'blog1', blog2 = 'blog2', blog3 = 'blog3')

def add(x, y):
    return x+y

c = [1, 5]

# print(add(*c))

def filter_df_test(titile, **kwargs):
    dict  = {}
    for kw, post in kwargs.items():
        dict[f'{kw}'] = post
    print(title+ f' {dict}')

# filter_df_test(title, BranchID='MR0001')

def filter_df(df, **kwargs):
    dict  = {}
    for kw, value in kwargs.items():
        dict[f'{kw}'] = value
    return df.loc[(df[list(dict)] == pd.Series(dict)).all(axis=1)]