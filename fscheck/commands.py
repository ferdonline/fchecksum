import fscheck
from docopt import docopt
from os import path as osp


def fsumcheck():
    """
    fsumcheck

    Usage:
        fsumcheck <file1> <file2> [<output>] [--spark-options=<opts>]

    Options:
        output Sets the output directory, or suppresses it (False) [default:fscheck_output]
        --spark-options=<opts> The options to be passed to Spark

    """

    args = _sanitize_opts(docopt(fsumcheck.__doc__, version='fsumcheck 0.1'))

    try:
        assert osp.isfile(args['file1']), "Input file 1 not found"
        assert osp.isfile(args['file2']), "Input file 2 not found"
    except AssertionError as e:
        print("Parameters error: \n - " + str(e))
        return

    optional_arg_names = ['output', 'spark_options']
    opt_args = {opt: args[opt] for opt in optional_arg_names if args[opt] is not None}

    fscheck.run(args['file1'], args['file2'], **opt_args)



def _sanitize_opts(docopt_opts):
    return {opt.strip("<>-").replace("-", "_"): val for opt, val in docopt_opts.items()}

