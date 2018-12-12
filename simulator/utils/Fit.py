from scipy import stats
import numpy as np
import matplotlib.pylab as plt
from simulator.utils.TraceToCSV import CSV_PATH


def weib(x, n, a):
    return (a / n) * (x / n)**(a - 1) * np.exp(-(x / n)**a)


file_name = CSV_PATH + "lanl_3_tran_ttf_node.csv"
data = np.loadtxt(file_name, delimiter='\n')
print data
(loc, scale) = stats.exponweib.fit(data, 1, 1)
print loc, scale

x = np.linspace(data.min(), data.max(), 1000)
plt.plot(x, weib(x, loc, scale))
# plt.hist(data, data.max())
plt.show()


# create some normal random noisy data
# ser = 50*np.random.rand() * np.random.normal(10, 10, 100) + 20

# plot normed histogram
# plt.hist(ser, normed=True)

# find minimum and maximum of xticks, so we know
# where we should compute theoretical distribution
# xt = plt.xticks()[0]
# xmin, xmax = min(xt), max(xt)
# lnspc = np.linspace(xmin, xmax, len(ser))

# lets try the normal distribution first
# get mean and standard deviation
# m, s = stats.norm.fit(ser)
# now get theoretical values in our interval
# pdf_g = stats.norm.pdf(lnspc, m, s)
# plot it
# plt.plot(lnspc, pdf_g, label="Norm")

# exactly same as above
# ag, bg, cg = stats.gamma.fit(ser)
# pdf_gamma = stats.gamma.pdf(lnspc, ag, bg,cg)
# plt.plot(lnspc, pdf_gamma, label="Gamma")

# guess what :)
# ab, bb, cb, db = stats.beta.fit(ser)
# pdf_beta = stats.beta.pdf(lnspc, ab, bb,cb, db)
# plt.plot(lnspc, pdf_beta, label="Beta")

# plt.show()
