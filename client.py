import rpyc

c = rpyc.connect("localhost", 5003)
print(c.root.is_leader())