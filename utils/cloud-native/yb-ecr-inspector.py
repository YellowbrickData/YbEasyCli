import boto3, json, argparse
from datetime import datetime, timezone, timedelta
from operator import itemgetter
from botocore.exceptions import ClientError

def img_info_dict(img):
    img_media_type  = img.get('imageManifestMediaType').split('.')
    img_tags        = img.get('imageTags')
    img_last_pulled = img.get('lastRecordedPullTime')
    img_dict = {
        'format' : img_media_type[1],
        'type'   : img_media_type[-2],
        'tag'    : img_tags[0] if img_tags else None,
        'digest' : img.get('imageDigest'),
        'size'   : img.get('imageSizeInBytes'),
        'pushed' : img.get('imagePushedAt').replace(microsecond = 0),
        'pulled' : img_last_pulled.replace(microsecond = 0) if img_last_pulled else None,
        'comps'  : {},
    }
    return img_dict

def img_info_str(img):
    time_diff: timedelta = (datetime.now(timezone.utc) - img['pulled']) if img['pulled'] else 'N/A'
    return f"size = {round(img['size']/1024**2,2):6.2f} MB, pushed {img['pushed']}, pulled {img['pulled'] if img['pulled'] else 'never'} ({str(time_diff).split('.')[0]:>18}) {short_digest(img['digest']):>24}"

def get_ecr_info(prefix, region_name):
    ecr_client = boto3.client('ecr', region_name=region_name)
    matching_repos = []
    try:
        paginator = ecr_client.get_paginator('describe_repositories')
        print(f"Searching for ECR repositories starting with '{prefix}' in {region_name} region ...")
        for page in paginator.paginate():
            for repo in page.get('repositories', []):
                repo_name = repo.get('repositoryName', '')
                if repo_name.startswith(prefix):
                    repo_info = { 'name': repo_name, 'uri': repo.get('repositoryUri'), 'created_at': repo.get('createdAt'), 'images': [] }
                    try:
                        image_paginator = ecr_client.get_paginator('describe_images')
                        for img_page in image_paginator.paginate(repositoryName=repo_name):
                            for img in img_page.get('imageDetails', []):
                                img_d = img_info_dict(img)
                                is_img_index = img_d['type'] in ('list', 'index',)
                                if is_img_index or (img_d['type'] == 'manifest' and img_d['tag']):
                                    if is_img_index:
                                        try:
                                            components = {}
                                            manifest_response = ecr_client.batch_get_image( repositoryName=repo_name, imageIds=[{'imageDigest': img_d['digest']}])
                                            for matched_img in manifest_response.get('images', []):
                                                # Inspect manifests array inside OCI index or Docker manifest list
                                                for manifest_ref in json.loads(matched_img.get('imageManifest', '{}')).get('manifests', []):
                                                    platform = manifest_ref.get('platform', {})
                                                    comp_digest = manifest_ref.get('digest')
                                                    components[comp_digest] = {'platform': f"{platform.get('os', 'unknown')}/{platform.get('architecture', 'unknown')}" + (f"/{platform.get('variant')}" if platform.get('variant') else ''),}
                                                for i in [img_info_dict(x) for x in ecr_client.describe_images(repositoryName = repo_name, imageIds = [{'imageDigest': x} for x in components.keys()]).get('imageDetails', [])]:
                                                    components[i['digest']] |= i
                                                img_d['comps'] = components
                                        except ClientError as manifest_err:
                                            print(f"Warning: Could not fetch manifest contents for digest '{img_d['digest']}': {manifest_err.response['Error']['Message']}")
                                    repo_info['images'].append(img_d)
                    except ClientError as img_err:
                        print(f"Warning: Could not fetch images for '{repo_name}': {img_err.response['Error']['Message']}")
                    if len(repo_info['images']) > 1:
                        matching_repos.append(repo_info)
        return matching_repos
    except ClientError as e:
        print(f"Error fetching ECR repositories: {e.response['Error']['Message']}")
        return []

short_digest = lambda s: f'{s[:13]}...{s[-6:]}'

if __name__ == "__main__":
    parser = argparse.ArgumentParser(prog = 'AWS ECR image inspector', description = 'Prints a report on all reporsitories with multiple images', formatter_class=argparse.RawTextHelpFormatter)
    # parser.add_argument('--version', action = 'version', version = '%(prog)s {v}'.format(v = __version__))
    parser.add_argument('-p', '--prefix', required = True, help = 'AWS ECR repository name prefix')
    parser.add_argument('-r', '--region', default = 'us-east-1', help = 'AWS region')
    parser.add_argument('-t', '--recent', type = int, default = None, help = 'How many most recent images to show (default: all)')
    args = parser.parse_args()

    repositories = get_ecr_info(prefix = args.prefix, region_name = args.region)

    def img_sort(x):
        if x['comps']:
            latest = max(x['comps'].values(), key = lambda y: y['pulled'].timestamp() if y['pulled'] else 0)['pulled']
        else:
            latest = x['pulled']
        return (latest is None, -latest.timestamp() if latest else 0)

    if repositories:
        print(f"\nFound {len(repositories)} matching repository/repositories:")
        for r in sorted(repositories, key = itemgetter('name')):
            print(f"Repo {r['name']} ({r['uri']}) images ({len(r['images'])} found):")
            for img in sorted(r['images'], key = img_sort)[:args.recent]:
                img_type = f"{img['format']}:{img['type']}"
                print(f"\ttag = {img['tag']:24s} type = {img_type:16s} {img_info_str(img)}")
                if img['comps']:
                    for digest, data in sorted(img['comps'].items(), key = lambda x: (x[1]['pulled'] is None, -x[1]['pulled'].timestamp() if x[1]['pulled'] else 0))[:args.recent]:
                        print(f"\t\tplatform: {data['platform']:<16s} {img_info_str(data)}")
    else:
        print(f"\nNo repositories found starting with '{args.prefix}' in {args.region} region.")
