from datetime import datetime
import errno
import io
import os
import stat

from tornado import (
    gen,
    web,
    iostream,
)
from tornado.log import app_log

from ...utils import url_path_join
from ..base import (
    cached,
    RenderingHandler,
)

import botocore
from .client import S3Client


class S3TreeHandler(RenderingHandler):
    """list files in a s3 (like file system)"""

    _localfile_path = '/s3bucket'

    @property
    def cli(self):
        if not hasattr(self, '_cli'):
            self._cli = S3Client()
        return self._cli

    @cached
    @gen.coroutine
    def get(self, path):
        """Get a directory listing, rendered notebook, or raw file
        at the given path based on the type and URL query parameters.

        If the path points to an accessible directory, render its contents.
        If the path points to an accessible notebook file, render it.
        If the path points to an accessible file and the URL contains a
        'download' query parameter, respond with the file as a download.

        Parameters
        ==========
        path: str
            Local filesystem path
        """
        fullpath = path

        # if not self.can_show(fullpath):
        #     app_log.info("path: '%s' is not visible from within nbviewer", fullpath)
        #     raise web.HTTPError(404)
        app_log.info('fullpath = ({})'.format(fullpath))
        if self.cli.is_dir(fullpath):
            html = self.show_dir(fullpath, path)
            raise gen.Return(self.cache_and_finish(html))

        is_download = self.get_query_arguments('download')
        if is_download:
            self.download(fullpath)
            return

        try:
            nbdata = self.cli.get_object(fullpath)['body'].read()
        except botocore.exceptions.ClientError as e:
            if e.response["Error"]["Code"] == "404":
                app_log.info("path : '%s' is not readable from within nbviewer", fullpath)
                raise web.HTTPError(404)
            raise e

        yield self.finish_notebook(nbdata,
                                   download_url='?download',
                                   msg="file from localfile: %s" % path,
                                   public=False,
                                   format=self.format,
                                   request=self.request,
                                   breadcrumbs=self.breadcrumbs(path),
                                   title=os.path.basename(path))

    def breadcrumbs(self, path):
        """Build a list of breadcrumbs leading up to and including the
        given local path.

        Parameters
        ----------
        path: str
            Relative path up to and including the leaf directory or file to include
            in the breadcrumbs list

        Returns
        -------
        list
            Breadcrumbs suitable for the link_breadcrumbs() jinja macro
        """
        breadcrumbs = [{
            'url': url_path_join(self.base_url, self._localfile_path),
            'name': 'home'
        }]
        breadcrumbs.extend(super(S3TreeHandler, self).breadcrumbs(path, self._localfile_path))
        return breadcrumbs

    def show_dir(self, fullpath, path):
        """Render the directory view template for a given filesystem path.

        Parameters
        ==========
        fullpath: string
            Absolute path on disk to show
        path: string
            URL path equating to the path on disk

        Returns
        =======
        str
            Rendered HTML
        """
        entries = []
        dirs = []
        ipynbs = []


        contents = self.cli.list(fullpath + '/')

        for content in contents:

            entry = {}
            entry['name'] = content['name']

            # We need to make UTC timestamps conform to true ISO-8601 by
            # appending Z(ulu). Without a timezone, the spec says it should be
            # treated as local time which is not what we want and causes
            # moment.js on the frontend to show times in the past or future
            # depending on the user's timezone.
            # https://en.wikipedia.org/wiki/ISO_8601#Time_zone_designators
            if content['type'] == 'dir':
                entry['modtime'] = '-'
                entry['url'] = url_path_join(self._localfile_path, path, content['name'])
                entry['class'] = 'fa fa-folder-open'
                dirs.append(entry)
            elif content['name'].endswith('.ipynb'):
                entry['modtime'] = content['last_modified'].isoformat() + 'Z'
                entry['url'] = url_path_join(self._localfile_path, path, content['name'])
                entry['class'] = 'fa fa-book'
                ipynbs.append(entry)

        dirs.sort(key=lambda e: e['name'])
        ipynbs.sort(key=lambda e: e['name'])

        entries.extend(dirs)
        entries.extend(ipynbs)

        html = self.render_template('dirview.html',
                                    entries=entries,
                                    breadcrumbs=self.breadcrumbs(path),
                                    title=url_path_join(path, '/'))
        return html

    @gen.coroutine
    def download(self, fullpath):
        """Download the file at the given absolute path.

        Parameters
        ==========
        fullpath: str
            Absolute path to the file
        """

        content = self.cli.get_object(fullpath)

        filename = fullpath.split('/')[-1]

        self.set_header('Content-Length', content['content_length'])
        # Escape commas to workaround Chrome issue with commas in download filenames
        self.set_header('Content-Disposition',
                        'attachment; filename={};'.format(filename.replace(',', '_')))

        for chunk in content['body'].iter_chunks():
            try:
                self.write(chunk)
                yield self.flush()
            except iostream.StreamClosedError:
                return


def default_handlers(handlers=[]):
    """Tornado handlers"""
    return handlers + [
        (r'/s3bucket/?(.*)', S3TreeHandler),
    ]
