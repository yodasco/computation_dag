decl {
    InputNode n1(format:"json", path:"t.json");
    ComputationNode n1(file:"my_logics.py", function:"my_data_function");
}
graph {
  n1 -> n1;
}
