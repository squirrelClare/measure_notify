"""
cron: 6 6 6 * *
new Env('量化--测试');
"""
from notify import send
# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    send("量表指标", "测试机内容", "仅供参考")

    a = [1, 2,3,2,4]
    s = ['\'{0}\''.format(e) for e in a]
    print(','.join(s))
# See PyCharm help at https://www.jetbrains.com/help/pycharm/
