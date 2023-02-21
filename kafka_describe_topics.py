# from kafka import KafkaAdminClient, TopicPartition
# from kafka import TopicPartition
from kafka.structs import TopicPartition
from kafka.admin import ConfigResource, KafkaAdminClient
from datetime import datetime
import tabulate
import argparse


def main(args):

    # Get data
    prop = args.property
    list_properties = args.list_properties
    client = KafkaAdminClient(bootstrap_servers=args.BROKERS)
    topics = client.list_topics()
    configs = {
        c[3]: dict([d[:2] for d in c[4]])
        for c in client.describe_configs(
            [ConfigResource("topic", topic) for topic in topics]
        )[0].resources
    }

    # List props?
    if list_properties:
        available_properties = set()
        for config in configs.values():
            available_properties |= config.keys()
        print(
            tabulate.tabulate(
                sorted([(k,) for k in available_properties]),
                headers=["Property Name"],
                tablefmt="simple",
            )
        )
        return

    # Dump data
    properties = []
    for name, config in configs.items():
        if ".ms" in prop:
            prop_ms_to_hours = int(config[prop]) // 1000 // 60 // 60
            properties.append(
                (
                    name,
                    config[prop],
                    prop_ms_to_hours,
                )
            )
        else:
            properties.append(
                (
                    name,
                    config[prop],
                )
            )

    if ".ms" in prop:
        print(
            tabulate.tabulate(
                sorted(properties, reverse=True, key=lambda k: int(k[1])),
                headers=["Topic", prop, "(in hours)"],
                tablefmt="simple",
            )
        )
    else:
        print(tabulate.tabulate(properties, headers=["Topic", prop], tablefmt="simple"))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("BROKERS", nargs="+")
    parser.add_argument("-p", "--property", default="retention.ms", type=str)
    parser.add_argument(
        "-l",
        "--list-properties",
        default=False,
        action="store_true",
        help="list available properties",
    )
    args = parser.parse_args()
    main(args)
