<style>
#signatureName {
  transition: opacity var(--fade-duration) ease-in-out;
  opacity: 1;
}
</style>

<script>
  // 🛠️ Configurable variables
  const fadeDuration = 1000; // ms
  const cycleInterval = 4000; // ms

  document.documentElement.style.setProperty('--fade-duration', fadeDuration + 'ms');

  function shuffleArray(array) {
    for (let i = array.length - 1; i > 0; i--) {
      const j = Math.floor(Math.random() * (i + 1));
      [array[i], array[j]] = [array[j], array[i]];
    }
  }

  fetch('/signatures.txt')
    .then(response => response.text())
    .then(data => {
      const lines = data.split('\n').map(l => l.trim()).filter(Boolean);
      const total = lines.length;
      shuffleArray(lines);

      let index = 0;
      const nameSpan = document.getElementById('signatureName');
      const totalSpan = document.getElementById('signatureCount');

      totalSpan.textContent = total;

      function updateText() {
        nameSpan.style.opacity = 0;

        setTimeout(() => {
          nameSpan.textContent = lines[index];
          index = (index + 1) % lines.length;
          nameSpan.style.opacity = 1;
        }, fadeDuration);
      }

      updateText();
      setInterval(updateText, cycleInterval);
    });
</script>

