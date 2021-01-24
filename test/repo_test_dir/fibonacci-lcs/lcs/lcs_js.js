const LCS = (longString, shortString) => {
    const build2DMatrix = () => {
      return Array(this.longStringLength + 1).fill()
        .map(_ => Array(this.shortStringLength + 1).fill(0));
    };
  
    const assignValues = () => {
      for (let row = 1; row < this.matrix.length; row++)
        for (let col = 1; col < this.matrix[row].length; col++) {
          if (longString[row-1] === shortString[col-1]) {
            this.matrix[row][col] = this.matrix[row-1][col-1] + 1;
          } else {
            this.matrix[row][col] = Math.max(this.matrix[row-1][col], this.matrix[row][col-1]);
          }
        }
    };
  
    const backtrace = (row, col) => {
      const currentGrid = this.matrix[row][col];
      if (currentGrid === 0) return;
  
      if (currentGrid === this.matrix[row][col-1]) {
        backtrace(row, col-1);
      } else if (currentGrid === this.matrix[row-1][col]) {
        backtrace(row-1, col);
      } else if (currentGrid === this.matrix[row-1][col-1] + 1) {
        this.results.push(longString[row-1]);
        backtrace(row-1, col-1);
      }
    };
  
    this.results = [];
    this.backtraceArray = [];
    this.longStringLength = longString.length;
    this.shortStringLength = shortString.length;
    this.matrix = build2DMatrix();
    assignValues();
    backtrace(this.longStringLength, this.shortStringLength);
  
    return this.results.reverse().join('');
  }
  
  const shortString1 = 'AGGTAB';
  const longString1 = 'GXTXAYB';
  const lcs1 = LCS(longString1, shortString1);
  console.log(lcs1)
  console.assert(lcs1 === 'GTAB', 'LCS must be GTAB');
  
  const shortString2 = 'AC';
  const longString2 = 'ABC';
  const lcs2 = LCS(longString2, shortString2);
  console.log(lcs2)
  console.assert(lcs2 === 'AC', 'LCS must be AC');
  
  const shortString3 = 'ABCDGH';
  const longString3 = 'AEDFHR';
  const lcs3 = LCS(longString3, shortString3);
  console.log(lcs3)
  console.assert(lcs3 === 'ADH', 'LCS must be ADH');