
// Define column definitions
// const columnDefs = [
//     { headerName: "Регистрационный номер", field: "registerNumber" },
//     { headerName: "Наименование типа", field: "typeName" },
//     { headerName: "Модификация", field: "modification" },
//     { headerName: "Тип", field: "type" },
//     { headerName: "Год", field: "year" }
// ];

const columnDefs = [
    { headerName: "Количество записей", field: "count" },
    { headerName: "Год", field: "year" }
];


let timeout = null;
let preventBlur = false; 
// Create the grid passing in the div to use together with the columns & data we want to use
const gridDiv = document.querySelector('#myGrid');
// new agGrid.Grid(gridDiv, gridOptions);  // Correctly initialize ag-Grid

function sendToAPI(inputData) {
    console.log(inputData);
    fetch('http://127.0.0.1:5000/statistics', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify(inputData),
    })
    .then(response => response.json())
    .then(data => {
        //console.log(data);
        
        const gridOptions = {
            //rowModelType: 'serverSide',
            columnDefs: columnDefs,
            rowData: data
            //rowData: []  // Initially set rowData to an empty array
        };
        
        new agGrid.Grid(gridDiv, gridOptions);

        //gridOptions.api.setRowData(data); !!!!!!!!!!!

        // if (gridOptions.api) {
        //     // gridOptions = {
        //     //     onGridReady: function() {
        //     //         gridOptions.api.setRowData(data);
        //     //     }}
        // } else {
        //     console.error("AG-Grid API не инициализировано");
        // }
        //gridOptions.api.setRowData(data);  // Update the grid with the new data
    })
    .catch(error => console.error('Error:', error));
}


function sendToAPIparam(inputData, list) {
    //console.log(777)
    //console.log(inputData);
    console.log(inputData);
    fetch('http://127.0.0.1:5000/imreciseSearch', {
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
        //console.log(input);
        for (const [k, valueArr] of Object.entries(data)) {
            //console.log(`${k}: ${valueArr}`);

            for (index = 0; index < valueArr.length; ++index) {
                //console.log(valueArr[index]);

                let option = document.createElement('div');
                option.id = valueArr[index];
                option.style.cursor = 'pointer';
                //parent = (option.parentElement);
                //console.log(parent);
                option.addEventListener('mousedown', function(event) {
                    //console.log('нажали на элемент списка');
                    input.value = event.target.textContent;
                    list.style.display = 'none';
                    preventBlur = true; // Предотвращаем пропадание списка (возникновение события blur)
                    //console.log(101101010);
                });
                option.textContent = valueArr[index];
                //console.log(list.parentNode)
                //option.className = 'dropdown-list';
                list.appendChild(option);
            }
        }
        //console.log(preventBlur);

        // field = document.getElementById('catched');
        // field.value = data;

    })
    .catch(error => console.error('Error:', error));
}

function SendData() {
    const inputData = {
        registerNumber: document.getElementById('registerNumber').value,
        typeName: document.getElementById('typeName').value,
        type: document.getElementById('type').value,
        modification: document.getElementById('modification').value
    };
    table = document.getElementById('myGrid');
    table.innerHTML = '';
    
    sendToAPI(inputData);
}


// Add input event listeners with a debounce
// document.querySelectorAll('.attribute-inputs input').forEach(inputField => {
//     inputField.addEventListener('input', function() {
//         clearTimeout(timeout);
//         timeout = setTimeout(() => {
//             const inputData = {
//                 registerNumber: document.getElementById('registerNumber').value,
//                 typeName: document.getElementById('typeName').value,
//                 type: document.getElementById('type').value,
//                 modification: document.getElementById('modification').value
//             };
//             sendToAPI(inputData);
//         }, 500);
//     });
// });


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
                //inputField.parentElement.querySelector('.list').style.display = 'flex';
                //listStyle = inputField.parentElement.querySelector('.list').style.display;
                inputField.addEventListener('blur', function() {
                    // Если клик был не на элемент списка, а в любое другое место
                    if (!preventBlur) {
                        inputField.parentElement.querySelector('.list').style.display = 'none'; // Скрываем контейнер, если потерян фокус
                    } else {
                        preventBlur = false;  // Сбрасываем флаг
                    }
                    // if (listStyle === 'none') {
                    //     listStyle = 'flex';
                    //     console.log('was none');
                    // } 
                    // else {
                    //     listStyle = 'none';
                    //     console.log('was flex');
                    // }
                    //inputField.parentElement.querySelector('.list').style.display = 'none';
                    //console.log('exit');
                });
                inputField.addEventListener('focus', function() {
                    if (inputField.value !== '' && inputField.value !== null) {
                        //preventBlur = false;
                        //console.log(inputField.value);
                        //console.log('asdasdasdasd');
                        inputField.parentElement.querySelector('.list').style.display = 'flex';
                    }
                });
            }
        }, 600);
    });
});



// for (const inp of document.querySelectorAll('.attribute-inputs input')) {
//     inp.addEventListener('input', getFullValue);
// }
  
// function getFullValue(event) {
// console.log(event.target.value)
    // clearTimeout(timeout);
    //     timeout = setTimeout(() => {
    //         const inputData = {
    //             registerNumber: document.getElementById('registerNumber').value,
    //             typeName: document.getElementById('typeName').value,
    //             type: document.getElementById('type').value,
    //             modification: document.getElementById('modification').value
    //         };
    //         sendToAPI(inputData);
    //     }, 500);
  //}
// document.querySelectorAll('.attribute-inputs input').forEach(inputField => {
//     inputField.addEventListener('input', function() {
//         clearTimeout(timeout);
//         timeout = setTimeout(() => {
//             const inputData = {
//                 registerNumber: document.getElementById('registerNumber').value,
//                 typeName: document.getElementById('typeName').value,
//                 type: document.getElementById('type').value,
//                 modification: document.getElementById('modification').value
//             };
//             sendToAPI(inputData);
//         }, 500);
//     });
// });




// function sendToAPI(inputData) {
//     fetch('http://127.0.0.1:5000/statistics', {
//         method: 'POST',
//         headers: {
//             'Content-Type': 'application/json',
//         },
//         body: JSON.stringify(inputData),
//     })
//     .then(response => response.json())
//     .then(data => {
//         dynamicallyConfigureColumnsFromObject(data[0]);
//         gridOptions.api.setRowData(data);
//     })
//     .catch(error => console.error('Error:', error));
// };

// function dynamicallyConfigureColumnsFromObject(data) {
//     var newColumnDefs = [
//         { headerName: 'typeName', field: 'typeName' },
//         { headerName: 'type', field: 'type' },
//         { headerName: 'registerNumber', field: 'registerNumber' },
//         { headerName: 'modification', field: 'modification', editable: true }
//       ];

//     if (gridOptions.api) {
//         gridOptions.api.setColumnDefs(newColumnDefs);
//     } else {
//         console.error('Grid API is not initialized yet.');
//     }
// };


// // document.addEventListener('DOMContentLoaded', function() {
// //     var gridOptions = {
// //         columnDefs: initialColumnDefs,
// //         rowData: null,
// //         onGridReady: function(params) {
// //             params.api.sizeColumnsToFit();
// //         }
// //     };

// //     var eGridDiv = document.querySelector('#myGrid');
// //     new agGrid.Grid(eGridDiv, gridOptions);
// // });
