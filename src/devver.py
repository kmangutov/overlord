import ast

class FunctionDocstringAnalyzer(ast.NodeVisitor):
    def visit_FunctionDef(self, node):
        # Check if the function has a docstring
        if ast.get_docstring(node):
            print(f"\nFunction name: {node.name}")
            print(f"Docstring: {ast.get_docstring(node)}")

            # Extract argument types from annotations
            arg_types = []
            for arg in node.args.args:
                arg_annotation = ast.unparse(arg.annotation) if arg.annotation else "Unknown"
                arg_types.append(f"{arg.arg}: {arg_annotation}")
            print(f"Input types: {arg_types}")

            # Extract return type from annotation
            return_type = ast.unparse(node.returns) if node.returns else "Unknown"
            print(f"Output type: {return_type}")

            # Extract and print the function's code
            function_code = ast.get_source_segment(module_content, node)
            print(f"Function code:\n{function_code}")

        # Continue traversing the AST
        self.generic_visit(node)
def analyze_module(file_path):
    global module_content
    with open(file_path, "r") as file:
        module_content = file.read()
    
    try:
        # Parse the module content
        tree = ast.parse(module_content)
    except SyntaxError as e:
        print(f"Syntax error in file {file_path}: {e}")
        return

    # Create an analyzer and visit the nodes in the tree
    analyzer = FunctionDocstringAnalyzer()
    analyzer.visit(tree)

# Example usage: analyze the current script
analyze_module("./src/pipeline_lib.py")