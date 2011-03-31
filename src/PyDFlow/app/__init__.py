'''
@author: Tim Armstrong
'''
import flowgraph
from flowgraph import App, outfiles
from decorator import app
from PyDFlow.compound import compound
from paths import add_path, set_paths
from PyDFlow.app.mappers import SimpleMapper, GlobMapper


flfile = flowgraph.FileChannel
localfile = flowgraph.LocalFileChannel
