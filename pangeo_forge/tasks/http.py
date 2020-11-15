import os

import fsspec
from prefect import task
import requests

@task
def download(source_url, cache_location, auth=()):
    """
    Download a remote file to a cache.

    Parameters
    ----------
    source_url : str
        Path or url to the source file.
    cache_location : str
        Path or url to the target location for the source file.

    Returns
    -------
    target_url : str
        Path or url in the form of `{cache_location}/hash({source_url})`.
    """
    target_url = os.path.join(cache_location, str(hash(source_url)))

    # there is probably a better way to do caching!
    try:
        fsspec.open(target_url).open()
        return target_url
    except FileNotFoundError:
        pass

    if len(auth) == 2:
        r = requests.get(source_url, auth=auth)
        with fsspec.open(target_url, mode="wb") as target:
            target.write(r.content)
    else:
        with fsspec.open(source_url, mode="rb", **kwargs) as source:
            with fsspec.open(target_url, mode="wb") as target:
                target.write(source.read())
    return target_url
