def fibs
f = [0, 1]
  13.times do |n|
    f.push((f[n].to_int + f[n + 1]).to_int)
  end
p f
end

#recursion
def fibs_rec(n)
    n <= 1? n : fibs_rec(n - 1) + fibs_rec(n - 2) 
end
