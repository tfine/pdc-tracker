// Debounced transcript search
(function () {
    const input = document.getElementById("transcript-q");
    if (!input) return;

    let timer = null;

    input.addEventListener("input", function () {
        clearTimeout(timer);
        timer = setTimeout(function () {
            if (input.value.trim().length >= 3) {
                input.closest("form").submit();
            }
        }, 500);
    });
})();
