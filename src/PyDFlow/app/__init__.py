import flowgraph
from decorator import app
from PyDFlow.compound import compound
from paths import add_path, set_paths



flfile = flowgraph.FileChannel
localfile = flowgraph.LocalFileChannel
