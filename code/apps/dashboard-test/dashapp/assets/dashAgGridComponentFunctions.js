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


dagcomponentfuncs.DCC_Input = function (props) {
    const {setData, value} = props.value;
    const {min, max, step} = props.colDef.cellRendererParams;

    // updated the dcc component
    setProps = ({value}) => {
       // update the grid
        props.node.setDataValue(props.column.colId, value);
        // update to trigger a dash callback
        setData(value);
    }

    return React.createElement(
        window.dash_core_components.Input, {
            type: "number",
            min: min,
            max: max,
            step: step,
            value: value,
            checked: value,
            setProps,
            style: {"paddingTop": 6},
        }
    )
};



dagcomponentfuncs.DBC_Input = function (props) {
    const {setData, value} = props.value;
    const {min, max, step} = props.colDef.cellRendererParams;

    // updated the dcc component
    setProps = ({value}) => {
       // update the grid
        props.node.setDataValue(props.column.colId, value);
        // update to trigger a dash callback
        setData(value)
    }

    return React.createElement(
        window.dash_bootstrap_components.Input, {
            type: "number",
            min: min,
            max: max,
            step: step,
            value: value,
            checked: value,
            setProps,
            style: {"paddingTop": 6},
        }
    )
};

// var dagcomponentfuncs = window.dashAgGridComponentFunctions = window.dashAgGridComponentFunctions || {};

// dagcomponentfuncs.DBC_Switch = function (props) {
//     const {setData, data} = props;

//     function onClick() {
//         setData();
//     }
//     return React.createElement(
//         window.dash_bootstrap_components.Switch,
//         {
//             onClick,
//             color: props.color,
//         },
//         props.value
//     );
// };