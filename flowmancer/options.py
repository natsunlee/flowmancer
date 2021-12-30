from optparse import OptionParser

parser = OptionParser()
parser.add_option("-j", "--jobdef", action="store", dest="jobdef")
parser.add_option("-r", "--restart", action="store_true", dest="restart", default=False)

(options, args) = parser.parse_args()

print(options.jobdef)
print(options.restart)