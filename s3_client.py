from aiofiles import open as aopen
from aiohttp import ClientResponse, ClientSession
from dataclasses import dataclass
from datetime import datetime
from typing import AsyncIterable, Self
from urllib.parse import urljoin
import os
import re


@dataclass(eq=True, frozen=True)
class RemoteFile:
    key: str
    last_modified: datetime
    size: int


class S3Client:
    _base_address: str
    _session: ClientSession

    def __init__(self, base_address: str) -> None:
        self._base_address = base_address
        self._session = ClientSession()

    async def enumerate_files(
        self, from_prefix: int = 0, to_prefix: int = 99
    ) -> AsyncIterable[RemoteFile]:
        CONTENTS_PATTERN = re.compile(r'<Contents>(.*?)</Contents>', re.S)

        for prefix in range(from_prefix, to_prefix + 1):
            async with self._session.get(
                self._base_address, params={'prefix': prefix}
            ) as response:
                text = await response.text()

                for match in CONTENTS_PATTERN.finditer(text):
                    yield self.__parse_remote_file(match[1])

    async def download_file(self, remote_file: RemoteFile, path: str = '.') -> None:
        url = urljoin(self._base_address, remote_file.key)

        async with self._session.get(url) as response:
            extension = self.__get_file_extension(response)
            filename = os.path.join(
                path,
                f'{remote_file.key}_{remote_file.last_modified:%Y-%m-%d__%H_%M_%S}{extension}',
            )

            async with aopen(filename, 'wb') as file:
                async for data in response.content.iter_any():
                    await file.write(data)

    async def close(self) -> None:
        await self._session.close()

    def __get_file_extension(self, response: ClientResponse) -> str | None:
        try:
            content_type = response.headers['content-type']
            extension = content_type[content_type.rfind('/') + 1 :]

            return f'.{extension}'
        except Exception:
            return None

    def __parse_remote_file(self, raw: str) -> RemoteFile:
        DATE_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
        KEY_PATTERN = re.compile(r'<Key>(.*?)</Key>', re.S)
        LAST_MODIFIED_PATTERN = re.compile(r'<LastModified>(.*?)</LastModified>', re.S)
        SIZE_PATTERN = re.compile(r'<Size>(.*?)</Size>', re.S)

        key = KEY_PATTERN.search(raw)
        if key is None:
            raise ValueError(f'Unable to get key from file. Content: {raw}')

        last_modified = LAST_MODIFIED_PATTERN.search(raw)
        if last_modified is None:
            raise ValueError(f'Unable to get last_modified from file. Content: {raw}')

        size = SIZE_PATTERN.search(raw)
        if size is None:
            raise ValueError(f'Unable to get size from file. Content: {raw}')

        return RemoteFile(
            key[1], datetime.strptime(last_modified[1], DATE_FORMAT), int(size[1])
        )

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, *_) -> None:
        await self.close()
