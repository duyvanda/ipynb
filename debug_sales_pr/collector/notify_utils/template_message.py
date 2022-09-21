def evolve_ignore_alarm(
    ignore_rules,
    execution_timestamp,
    interval_seconds,
    latest_timestamp=None,
    continue_num=None,
    max_continue=2,
):
    if continue_num is None:
        continue_num = 0
    else:
        continue_num = int(continue_num)
    ignore_time = ignore_rules[str(continue_num)]

    if latest_timestamp:
        if execution_timestamp < latest_timestamp + ignore_time:
            print("ignore", latest_timestamp, continue_num, ignore_time)
            return latest_timestamp, continue_num
        if execution_timestamp <= latest_timestamp + ignore_time + interval_seconds:
            continue_num = continue_num + 1
            if continue_num > max_continue:
                continue_num = max_continue
    print("alarm", execution_timestamp, latest_timestamp, continue_num)
    return execution_timestamp, continue_num


# if __name__ == '__main__':
#     import datetime
#     latest_timestamp = None
#     continue_num = None
#     execution_date = datetime.datetime(2020, 1, 1, 0, 0)
#     ignore_rules = {
#         "0": 0,
#         "1": 600,
#         "2": 1800,
#     }
#     interval_seconds = 60
#     for i in range(100):
#         t = execution_date + datetime.timedelta(seconds=interval_seconds*i)
#         print(t)
#         latest_timestamp, continue_num = evolve_ignore_alarm(
#             ignore_rules, t.timestamp(), interval_seconds, latest_timestamp, continue_num)

#     dct_test1 = {
#         "log_time": "1",
#         "frequency": "1"
#     }
#     dct_test2 = {
#         "log_time": "1",
#         "frequency": "1",
#         "test": "2"
#     }
#     dct_test3 = {
#         "log_time": "1"
#     }
#     print(render_template(CREATE_ORDER_MSG_RULE_01, dct_test1))
#     print(render_template(CREATE_ORDER_MSG_RULE_01, dct_test2))
#     print(render_template(CREATE_ORDER_MSG_RULE_01, dct_test3))

# env = Environment()
# parsed_content = env.parse("{{ log_time }} no found data, number frequency {{ frequency }}")
# print(meta.find_undeclared_variables(parsed_content))
#     import datetime
#     log_time = datetime.datetime.now()
#     dct_args = {"log_time": log_time}
#
#     print(CREATE_ORDER_MSG_RULE_01.render(**dct_args))
