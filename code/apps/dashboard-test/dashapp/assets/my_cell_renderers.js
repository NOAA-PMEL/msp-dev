// assets/my_cell_renderers.js
// var dagcomponentfuncs = window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {};

// dagcomponentfuncs.PowerButtonRenderer = function(props) {
//     const { value, data } = props; // value is the 'on' state, data is the row

//     // You'll need to dynamically create the Dash DAQ PowerButton here
//     // This is a simplified representation, actual implementation requires more React setup
//     return React.createElement(window.dash_daq.PowerButton, {
//         id: `power-button-${data.id}`, // Example ID based on row data
//         on: value,
//         // Add other PowerButton props as needed, e.g., color, label
//         // You'll also need to handle the 'on' state change and send it back to Dash
//         // This usually involves dispatching a custom event or using a Dash callback
//     });
// };

var dagcomponentfuncs = window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {};

dagcomponentfuncs.DBC_Switch = function (props) {
    const {setData, value} = props;

    // updated the dbc component
    setProps = ({value}) => {
       // update the grid
        props.node.setDataValue(props.column.colId, value);
        // update to trigger a dash callback
        setData(value)
    }

    return React.createElement(
        window.dash_bootstrap_components.Switch, {
            value: Boolean(value),
            checked: value,
            setProps,
            style: {"paddingTop": 6},
        }
    )
};

// var dagcomponentfuncs = (window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {});

// dagcomponentfuncs.launchBtn = function (props) {
//     console.log('launchBtn', props)
//     return React.createElement('a',
//     {   target: '_blank',
//         href: props.value,
//         className: `block h-[40px] text-center text-md text-white rounded no-underline cursor-pointer hover:bg-blue-600`
//     }, 'Launch')
// };