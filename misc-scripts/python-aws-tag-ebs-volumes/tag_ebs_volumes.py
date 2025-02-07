import argparse
import boto3
import os
import fnmatch
from typing import Dict, List, Set


def main():
    print("\n===================================")
    print("AWS Tag EBS volumes by Alex Dinh")
    print("===================================")

    parser = get_parser()
    global opts
    opts = parser.parse_args()

    aws_profile = opts.profile
    aws_region = opts.region

    instance_ids = opts.instance_ids
    ebs_volume_ids = opts.volume_ids

    if opts.file:
        ebs_volume_id_file = get_full_path_to(opts.file)
        with open(ebs_volume_id_file, "r") as _f:
            ebs_volume_ids.append(_f.read().splitlines())

    print("Using the following as script input:")
    print(f"  - AWS Profile: {aws_profile}")
    print(f"  - AWS Region: {aws_region}")
    print(f"  - EBS Volume IDs: {ebs_volume_ids}")
    print(f"  - Instance IDs: {instance_ids}")

    if False and not get_confirmation("\nContinue with the script?"):
        print("\nYou have chosen to stop the script.")
        exit()

    ec2_resource, ec2_client = connect_to_aws(aws_profile, aws_region)

    print(" \nBegin tagging EC2 volumes!")
    start_tagging_volumes(
        ec2_resource,
        ec2_client,
        ebs_volume_ids,
        instance_ids,
        opts.tags,
        overwrite=opts.overwrite,
        verbose=opts.verbose,
        dry_run=opts.dry_run,
    )

    print("\nScript completed!")


def get_full_path_to(input_path):
    script_dir = os.path.dirname(os.path.abspath(__file__))
    file_name = input_path.split("/")[-1]

    # First check if is next to the script file
    if os.path.exists(f"{script_dir}/{file_name}"):
        return f"{script_dir}/{file_name}"
    # Check the actual input path
    elif os.path.exists(input_path):
        return input_path
    else:
        raise FileNotFoundError


def connect_to_aws(aws_profile, aws_region):
    print("\nConnecting into AWS...")
    try:
        aws_session = boto3.Session(profile_name=aws_profile, region_name=aws_region)
        client = aws_session.client("sts")
        caller_arn = client.get_caller_identity()["Arn"]
    except Exception as e:
        print(e)
        exit(1)

    print(f"Successfully connected into AWS using - {caller_arn}")
    ec2_resource = aws_session.resource("ec2", region_name=aws_region)
    ec2_client = aws_session.client("ec2", region_name=aws_region)

    return ec2_resource, ec2_client


def filter_tags(tags: Dict[str, str], filter_tags: List[str]) -> Dict[str, str]:
    """
    Filter the tags based on the filter_tags list
    """
    return {
        key: value
        for key, value in tags.items()
        if any(fnmatch.fnmatch(key, tag) for tag in filter_tags)
    }


def start_tagging_volumes(
    ec2_resource,
    ec2_client,
    ebs_volume_ids,
    instance_ids,
    tags,
    overwrite,
    verbose,
    dry_run,
):
    # tags_by_instance is a dictionary which contains the instance ID as the key and the tags as the value
    tags_by_instance: Dict[str, Dict[str:str]] = dict()
    volumes_by_instance: Dict[str, Set[str]] = dict()

    # iterate over the supplied instances and get the tags and their volumes
    found_volumes: Set[str] = set()
    for instance_id in set(instance_ids):
        tags_by_instance[instance_id] = get_resource_tags(
            ec2_client, "instance", instance_id
        )
        volumes_by_instance[instance_id] = set(
            get_instance_volumes(ec2_resource, instance_id, verbose=verbose)
        )
        found_volumes.update(volumes_by_instance[instance_id])

    # iterate over the supplied volumes and find the attached instance
    for vol_id in set(ebs_volume_ids):
        if vol_id in found_volumes:
            continue
        ebs_volume = ec2_resource.Volume(vol_id)
        if ebs_volume.state == "available":
            # The volume status is 'available', therefore it is not attached to anything
            print(f"Volume ID [{vol_id}] has no attachments")
            print("Skipped tagging this volume")
            continue
        instance_id = ebs_volume.attachments[0].get("InstanceId")
        if instance_id not in tags_by_instance:
            tags_by_instance[instance_id] = get_resource_tags(
                ec2_client, "instance", instance_id, verbose=verbose
            )
            volumes_by_instance[instance_id] = set([vol_id])
            found_volumes.add(vol_id)

    # now, iterate over instances
    for instance_id, volumes in volumes_by_instance.items():
        tags_on_instance = tags_by_instance[instance_id]
        instance_name = tags_on_instance.get("Name", "N/A")
        if tags:
            tags_on_instance = filter_tags(tags_on_instance, tags)

        print("================================================================")
        print(f"Processing instance [{instance_id}], Name: {instance_name!r}")
        print(f"Instance tags{" (filtered)" if tags else ""}: {tags_on_instance}")
        print(f"Processing volumes: {volumes}")

        for volume_id in volumes:
            print("--------------------------------")
            ebs_volume = ec2_resource.Volume(volume_id)
            tags_on_volume = tags_to_dict(ebs_volume.tags)
            volume_name = tags_on_volume.get("Name", "N/A")
            if tags:
                tags_on_volume = filter_tags(tags_on_volume, tags)
            print(f"Processing volume [{volume_id}], Name: {volume_name!r}")

            new_tags = {
                key: value
                for key, value in tags_on_instance.items()
                if key not in tags_on_volume
            }
            same_tags = {
                key: value
                for key, value in tags_on_instance.items()
                if key in tags_on_volume and tags_on_volume[key] == value
            }
            differing_tags = {
                key: value
                for key, value in tags_on_instance.items()
                if key in tags_on_volume and tags_on_volume[key] != value
            }
            differing_tags_on_volume = {
                key: value
                for key, value in tags_on_volume.items()
                if key in differing_tags
            }
            missing_tags = {
                key: value
                for key, value in tags_on_volume.items()
                if key not in tags_on_instance
            }

            if overwrite:
                tags_to_apply = {**new_tags, **differing_tags}
            else:
                tags_to_apply = new_tags

            if verbose:
                if same_tags:
                    print(f"Found identical tags: {same_tags}")
                if missing_tags:
                    print(f"Found tags on volume missing from instance: {missing_tags}")
                if tags_to_apply and overwrite:
                    print(f"Adding tags {new_tags} and updating tags {differing_tags}")
                elif tags_to_apply:
                    if differing_tags_on_volume:
                        print(
                            f"Adding tags {new_tags}, not overwriting {differing_tags_on_volume} with {differing_tags}"
                        )
                    else:
                        print(f"Adding tags {new_tags}")

            if not tags_to_apply:
                print("No tags to apply")
                continue

            if not dry_run:
                print(f"Tagging volume with the following tags: {tags_to_apply}")
                ec2_client.create_tags(
                    Resources=[volume_id], Tags=dict_to_tags(tags_to_apply)
                )
            else:
                print(f"Dry run, not tagging volume with tags: {tags_to_apply}")


def tags_to_dict(tags):
    return {tag["Key"]: tag["Value"] for tag in tags}


def dict_to_tags(tags):
    return [{"Key": key, "Value": value} for key, value in tags.items()]


def get_resource_tags(
    ec2_client, resource_type: str, resource_id: str, verbose: bool = False
) -> Dict[str, str]:
    instance_tags = ec2_client.describe_tags(
        Filters=[{"Name": "resource-id", "Values": [resource_id]}]
    )["Tags"]

    tags = tags_to_dict(instance_tags)
    if verbose:
        print(
            f"The {resource_type} [{resource_id}] contains the following tags: {tags}"
        )

    return tags


def get_instance_volumes(ec2_resource, instance_id: str, verbose=True) -> List[str]:
    instance = ec2_resource.Instance(instance_id)
    volumes = list()

    for volume in instance.volumes.all():
        volumes.append(volume.id)

    if verbose:
        print(f"Adding volumes {volumes} from instance {instance_id}")

    return volumes


def get_confirmation(prompt):
    input_text = input(f"{prompt} (yes/no): ")

    options = {"yes": True, "no": False}

    try:
        return options[input_text]
    except KeyError:
        print("Bad input, try again")
        get_confirmation(prompt)


def get_parser():
    parser = argparse.ArgumentParser(description="Assume role in AWS.")
    parser.add_argument(
        "-p",
        "--profile",
        required=False,
        help="The AWS profile name (default is 'default')",
        default="default",
    )
    parser.add_argument(
        "-r",
        "--region",
        required=True,
        help="The AWS region which the volume ID's are located. e.g. ap-southeast-2",
    )
    parser.add_argument(
        "-f", "--file", required=False, help="File containing the EBS volume ID's"
    )
    parser.add_argument(
        "--instance-ids",
        nargs="+",
        required=False,
        default=[],
        help="The EC2 instance IDs to tag the EBS volumes",
    )
    parser.add_argument(
        "--volume-ids",
        nargs="+",
        required=False,
        default=[],
        help="The EBS volume IDs to tag",
    )
    parser.add_argument(
        "--tags",
        nargs="+",
        required=False,
        help="The tags to apply to the EBS volumes, can contain wildcards",
    )
    parser.add_argument(
        "--overwrite", action="store_true", help="Overwrite the existing tags"
    )
    parser.add_argument("--dry-run", action="store_true", help="Dry run the script")
    parser.add_argument("--verbose", "-v", action="store_true", help="Verbose output")

    return parser


if __name__ == "__main__":
    main()
