const https = require('https');
const fs = require('fs');

const options = {
  hostname: 'zamzamcapital.in',
  path: '/shariah-compliant-stocks/',
  headers: { 'User-Agent': 'Mozilla/5.0' }
};

https.get(options, (res) => {
  let data = '';
  res.on('data', chunk => data += chunk);
  res.on('end', () => {
    // Extract potential NSE symbols
    const symbols = new Set();
    const words = data.replace(/<[^>]+>/g, ' ').split(/\s+/);
    words.forEach(word => {
      const clean = word.replace(/[^A-Z&-]/g, '');
      if (clean.length >= 2 && clean.length <= 20 &&
          /^[A-Z&-]+$/.test(clean) && !/^\d+$/.test(clean)) {
        symbols.add(clean);
      }
    });

    const cache = {
      updated: new Date().toISOString().split('T')[0],
      count: symbols.size,
      symbols: [...symbols].sort()
    };

    fs.writeFileSync('zamzam_cache.json', JSON.stringify(cache, null, 2));
    console.log(`Saved ${symbols.size} symbols to zamzam_cache.json`);
  });
}).on('error', e => console.error('Error:', e.message));