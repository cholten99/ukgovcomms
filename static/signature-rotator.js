const displayTime = 3000; // Time each signature is shown (ms)

const items = document.querySelectorAll('.signature-item');
let currentIndex = 0;

function showSignature(index) {
    items.forEach((el, i) => {
        el.classList.toggle('active', i === index);
    });
}

if (items.length > 0) {
    showSignature(0);
    setInterval(() => {
        currentIndex = (currentIndex + 1) % items.length;
        showSignature(currentIndex);
    }, displayTime);
}

