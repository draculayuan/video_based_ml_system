from consumeANDsave import consume

db_info = {
        'host': 'localhost',
        'user': 'yuan',
        'passwd': 'Liuyuan980820',
        'db': 'sentiment'
        }
consume('display', 'earliest', db_info)
