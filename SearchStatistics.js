const columnDefs = [
    { headerName: "Количество записей", field: "count" },
    { headerName: "Год", field: "year" }
];


let timeout = null;
let preventBlur = false; 
// Create the grid passing in the div to use together with the columns & data we want to use
const gridDiv = document.querySelector('#myGrid');


function sendToAPI(inputData) {
    // Показать индикатор загрузки
    document.getElementById('loading').style.display = 'block';
    console.log('do block')

    fetch('https://192.168.0.9:443/statistics', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(inputData),
    })
    .then(response => response.json())
    .then(data => {
        const gridOptions = {
            columnDefs: columnDefs,
            rowData: data
        };
        
        new agGrid.Grid(gridDiv, gridOptions);
    })
    .catch(error => console.error('Error:', error))
    .finally(() => {
        // Скрыть индикатор загрузки
        document.getElementById('loading').style.display = 'none';
        console.log('do none')
    });
}

function SendData() {
    const inputData = {
        mit_number: document.getElementById('mit_number').value,
        mit_title: document.getElementById('mit_title').value,
        mit_notation: document.getElementById('mit_notation').value,
        mi_modification: document.getElementById('mi_modification').value
    };

    // Очищаем таблицу перед отправкой данных
    table = document.getElementById('myGrid');
    table.innerHTML = '';

    sendToAPI(inputData);
}



function sendToAPIparam(inputData, list) {
    //fetch('http://192.168.56.1:5000/imreciseSearch', {
    fetch('https://192.168.0.9:443/imreciseSearch', {
        //fetch('http://192.168.0.16:80/imreciseSearch', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(inputData),
    })
    .then(response => response.json())
    .then(data => {
        console.log(data);
        list.innerHTML = '';
        list.style.display = 'flex';
        input = list.parentNode.querySelector('.mainInput');
        for (const [k, valueArr] of Object.entries(data)) {

            for (index = 0; index < valueArr.length; ++index) {

                let option = document.createElement('div');
                option.id = valueArr[index];
                option.style.cursor = 'pointer';

                option.addEventListener('mousedown', function(event) {
                    //console.log('нажали на элемент списка');
                    input.value = event.target.textContent;
                    list.style.display = 'none';
                    preventBlur = true; // Предотвращаем пропадание списка (возникновение события blur)
                });
                option.textContent = valueArr[index];
                list.appendChild(option);
            }
        }

    })
    .catch(error => console.error('Error:', error));
}

document.querySelectorAll('.attribute-inputs input').forEach(inputField => {
    inputField.addEventListener('input', function(event) {
        clearTimeout(timeout);
        timeout = setTimeout(() => {
            var key = event.target.getAttribute('id');
            var list = document.getElementById(key + 'List');
            var v = event.target.value;
            if (v !== null && v !== '') {
                //console.log(v);
                sendToAPIparam({[key]: v}, list);

                inputField.addEventListener('blur', function() {
                    // Если клик был не на элемент списка, а в любое другое место
                    if (!preventBlur) {
                        inputField.parentElement.querySelector('.list').style.display = 'none'; // Скрываем контейнер, если потерян фокус
                    } else {
                        preventBlur = false;  // Сбрасываем флаг
                    }
                });
                inputField.addEventListener('focus', function() {
                    if (inputField.value !== '' && inputField.value !== null) {
                        inputField.parentElement.querySelector('.list').style.display = 'flex';
                    }
                });
            }
        }, 600);
    });
});

