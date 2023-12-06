from aiofiles.os import path
from argparse import ArgumentParser
from asyncio import run, gather
from dataclasses import dataclass
from s3_client import RemoteFile, S3Client
from utils import async_chunks


@dataclass(frozen=True, eq=True)
class DumpArgs:
    url: str
    batch_count: int | None = None
    output_dir: str | None = None
    from_prefix: int | None = None
    to_prefix: int | None = None


def parse_arguments() -> DumpArgs:
    parser = ArgumentParser(
        'S3Dumper', description='Python script for dump data from open S3 bucket.'
    )

    parser.add_argument(
        '-b', '--batch-count', help='number of files downloaded simultaneously'
    )
    parser.add_argument('-o', '--output', help='output directory for dump files')
    parser.add_argument('-f', '--from-prefix', help='start dump from provided prefix')
    parser.add_argument('-t', '--to-prefix', help='continue dump to provided prefix')
    parser.add_argument('url', nargs=1, help='URL to the meeting')

    args = parser.parse_args()
    return DumpArgs(args.url[0], args.batch_count, args.output)


async def safe_download_file(
    client: S3Client, remote_file: RemoteFile, path: str = '.'
) -> None:
    while True:
        try:
            await client.download_file(remote_file, path)
            return
        except Exception:
            pass


async def main(args: DumpArgs) -> None:
    async with S3Client(args.url) as client:
        dumped_count = 0
        batch_count = args.batch_count or 20
        output_dir = args.output_dir or '.'

        if not await path.exists(output_dir):
            print(f'[!] Unable to find directory "{output_dir}".')
            return

        from_prefix = args.from_prefix or 0
        to_prefix = args.to_prefix or 99

        async for files in async_chunks(
            aiter(client.enumerate_files(from_prefix, to_prefix)), batch_count
        ):
            tasks = (safe_download_file(client, file, output_dir) for file in files)
            await gather(*tasks)

            dumped_count += len(files)
            print(f'[*] Dumped {dumped_count} files. . .')


run(main(parse_arguments()))
