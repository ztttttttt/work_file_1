from igraph import *


class MlxGraph:
    def __init__(self):
        super().__init__()
        self.g = Graph()
        self.delete = 0
        self.edge_success = 0

    def build(self, vertices, edges):
        """
        :param vertices: dict list like [{'name': 'v1_name','age': v1_age}, {'name': 'v2_name','age': v2_age}, ...],
                         name is a must key, and the value is unique
        :param edges: dict list like [{'source' : 'v1_name', 'target' : 'v2_name', 'attr1' : attr1},
                                      {'source' : 'v3_name', 'target' : 'v4_name', 'attr1' : attr1},
                                      ...]
                      source and target are must keys, source value <> target value
        """
        self.g = Graph()
        for v in vertices:
            self.g.add_vertex(**v)

        for e in edges:
            self.delete += 1
            print(self.delete)  # 记录边处理数量,其实是为了占用窗口,防止失联,
            try:
                self.g.add_edge(**e)
                self.edge_success += 1
                print("load edge successful! {}".format(self.edge_success))
            except ValueError:  # 由于时间控制,顶点可能不够,加入边的时候,不存在的顶点会报错
                pass

        return self.g.vcount(), self.g.ecount()

    def find_vertex(self, vertex_name):
        """
        :param vertex_name: vertex name
        :return: Vertex object
        """
        return self.g.vs.find(vertex_name)

    def find_subgraph(self, vertex_name, edge_type=None):
        """
        :param vertex_name: vertex name: str
        :param edge_type: str
        :return: Graph object
        """
        if not edge_type:
            return self.g.subgraph(self.g.subcomponent(vertex_name))
        else:
            subg = self.g.subgraph(self.g.subcomponent(vertex_name))
            eds = subg.es(type_eq=edge_type)
            vers = []
            for e in eds:
                vers.append(e.source)
                vers.append(e.target)
            vers = list(set(vers))
            subg = subg.subgraph(vers)
            if vertex_name in subg.vs['name']:
                return subg.subgraph(subg.subcomponent(vertex_name))
            else:
                return None

    def add_vertex(self, vertex_name, **attrs):
        self.g.add_vertex(vertex_name, **attrs)

    def add_edge(self, source, target, **attrs):
        if source not in self.g.vs['name']:
            self.g.add_vertex(source)
        if target not in self.g.vs['name']:
            self.g.add_vertex(target)
        self.g.add_edge(source, target, **attrs)

    def save_graph(self, file_name):
        self.g.write_pickle(file_name)

    def load_graph(self, file_name):
        self.g = self.g.Read_Pickle(file_name)
        return self.g.vcount(), self.g.ecount()

    def edge_exists(self, source_name, target_name, edge_type):
        if source_name not in self.g.vs['name'] or target_name not in self.g.vs['name']:
            return False
        edge_seqs = self.g.es(_within=[self.g.vs.find(source_name).index, self.g.vs.find(target_name).index])
        return True if edge_type in edge_seqs['type'] else False
