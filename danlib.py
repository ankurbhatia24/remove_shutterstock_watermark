import mimetypes
import requests

class Danbooru:
    def __init__(self, proxies):
        self.proxies = proxies

    class DanbooruError(Exception):
        pass

    def get_post(self, videometa):
        id = videometa['id']
        url = videometa['videourl']
        r = requests.get(url, stream=True)
        extension = mimetypes.guess_extension(r.headers['content-type'])
        if extension not in {'.mp4', '.webm'}:
            raise self.DanbooruError(f'Invalid Extension {extension}!')
        if r.status_code != 200:
            raise self.DanbooruError(f'Server returned {r.status_code} for ID {id} (image download)!')
        r.raw.decode_content = True
        return r.raw, extension
