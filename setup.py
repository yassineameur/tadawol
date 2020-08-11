import setuptools

setuptools.setup(
    setup_requires=['pbr', 'wheel', 'Click'],
    entry_points='''
        [console_scripts]
        tadawol=commands:cli
    '''
)
