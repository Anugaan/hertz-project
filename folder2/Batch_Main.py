from auditing import Driver
import argparse

if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument("--operation_type", help="Determines whether to run batch start/end", required=True)
    parser.add_argument("--abc_config", help="ABC YAML", required=True)
    parser.add_argument("--batch_identifier", help="identifier", required=True)
    parser.add_argument("--batch_number", help="batch_number")
    parser.add_argument("--real_time_flag", help="flag indicating whether the flow is real time or not")
    args = parser.parse_args()
    Driver.kinesis_main(args.operation_type,None,args.abc_config,args.batch_identifier,args.batch_number,args.real_time_flag)
