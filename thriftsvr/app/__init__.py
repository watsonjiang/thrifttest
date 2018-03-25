# -*- coding: utf-8 -*-

class ThriftApplication(object):
    '''
       应用信息容器
       tfactory thrift transport factory
       pfactory thrift protocol factory
    '''
    def __init__(self, processor, tfactory, pfactory):
        self.processor = processor
        self.tfactory = tfactory
        self.pfactory = pfactory