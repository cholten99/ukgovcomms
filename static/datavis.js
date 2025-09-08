document.addEventListener('DOMContentLoaded', () => {
  const overlay  = document.getElementById('img-modal');
  const imgEl    = document.getElementById('img-modal-img');
  const caption  = document.getElementById('img-modal-caption');
  const closeBtn = document.getElementById('img-modal-close');

  // Make the three charts clickable
  document.querySelectorAll('.global-viz-row .viz img').forEach(img => {
    img.style.cursor = 'zoom-in';
    img.addEventListener('click', () => {
      imgEl.src = img.src;
      imgEl.alt = img.alt || '';
      caption.textContent = img.nextElementSibling ? img.nextElementSibling.textContent : '';
      overlay.classList.add('open');
      overlay.setAttribute('aria-hidden', 'false');
      // prevent page scroll while open
      document.body.style.overflow = 'hidden';
    });
  });

  const close = () => {
    overlay.classList.remove('open');
    overlay.setAttribute('aria-hidden', 'true');
    imgEl.src = '';
    document.body.style.overflow = '';
  };

  // click outside image to close
  overlay.addEventListener('click', (e) => { if (e.target === overlay) close(); });
  closeBtn.addEventListener('click', close);
  document.addEventListener('keydown', (e) => { if (e.key === 'Escape') close(); });
});
