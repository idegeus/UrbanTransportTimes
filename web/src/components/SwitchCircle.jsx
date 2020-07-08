import React from 'react';
import styled from 'styled-components'
import {Spring} from 'react-spring/renderprops'

export const SwitchCircle = (props) => {
    // Element to display the switching point as a 
    // radius from the center. 

    let switching_point = props.point || 0
    let switching_scale = props.scale || 9.5
    let anim_factor = props.anim_factor || 1
    let cityname = props.cityname
    let switching_fact = (switching_point / switching_scale)
    let circleradius = Math.min(switching_fact, 0.95) / 2 * 100
    let is_max = (switching_point >= switching_scale)
    let width = props.width | "150"
    let height = props.height | props.width + 35
    let styles = Object.assign({}, props.style, {
        width, 
        height, 
        margin: 20,
    })

    return (
        <div style={{margin: "1.6rem"}}>
            <svg
                width={width} height={width}
                viewBox="0 0 100 100" preserveAspectRatio="none">

                {/* Masks out the top right corner for text. */}
                <mask id="sqrCutout">
                <polygon points="0 0  0 100  100 100  100 50  50 50  50 0"
                        fill="white"/>
                </mask>

                {/* Text titles for elements (including min if needed) */}
                {is_max && <text x="56" y="13" fontSize="10"
                    fill="#87847E">min.</text>}
                <text x="56" y="30" fontSize="16"
                    fill="#403D39">{switching_point}km</text>

                {/* Draws the actual circles here. */}
                <circle
                    cx="50" cy="50"
                    // r={50} 
                    r={anim_factor * 50} 
                    fill='#1783BB'
                    mask="url(#sqrCutout)" /> 
                <circle
                    cx="50%" cy="50%"
                    r={anim_factor * circleradius}
                    // r={circleradius}
                    stroke='#FFFCF2'
                    strokeWidth='1px'
                    fill='#EC6A37'
                    mask="url(#sqrCutout)" />

                {/* Sometimes a house or a tree. */}
                {switching_fact < 0.4 && <path 
                    transform="translate(75,34) scale(0.8)" 
                    fill='rgba(65, 62, 58, 0.14)'
                    d="M10,20V14H14V20H19V12H22L12,3L2,12H5V20H10Z" />}
                {switching_fact > 0.8 && <path
                    transform="translate(58,34) scale(0.8)" 
                    fill='rgba(65, 62, 58, 0.14)'
                    d="M11,21V16.74C10.53,16.91 10.03,17 9.5,17C7,17 5,15 5,12.5C5,11.23 5.5,10.09 6.36,9.27C6.13,8.73 6,8.13 6,7.5C6,5 8,3 10.5,3C12.06,3 13.44,3.8 14.25,5C14.33,5 14.41,5 14.5,5A5.5,5.5 0 0,1 20,10.5A5.5,5.5 0 0,1 14.5,16C14,16 13.5,15.93 13,15.79V21H11Z" />}

                {/* Draws the bike, car. */}
                <path 
                    transform="translate(52,37.5) scale(0.6)" 
                    fill='#EC6A37'
                    d="M5,20.5A3.5,3.5 0 0,1 1.5,17A3.5,3.5 0 0,1 5,13.5A3.5,3.5 0 0,1 8.5,17A3.5,3.5 0 0,1 5,20.5M5,12A5,5 0 0,0 0,17A5,5 0 0,0 5,22A5,5 0 0,0 10,17A5,5 0 0,0 5,12M14.8,10H19V8.2H15.8L13.86,4.93C13.57,4.43 13,4.1 12.4,4.1C11.93,4.1 11.5,4.29 11.2,4.6L7.5,8.29C7.19,8.6 7,9 7,9.5C7,10.13 7.33,10.66 7.85,10.97L11.2,13V18H13V11.5L10.75,9.85L13.07,7.5M19,20.5A3.5,3.5 0 0,1 15.5,17A3.5,3.5 0 0,1 19,13.5A3.5,3.5 0 0,1 22.5,17A3.5,3.5 0 0,1 19,20.5M19,12A5,5 0 0,0 14,17A5,5 0 0,0 19,22A5,5 0 0,0 24,17A5,5 0 0,0 19,12M16,4.8C17,4.8 17.8,4 17.8,3C17.8,2 17,1.2 16,1.2C15,1.2 14.2,2 14.2,3C14.2,4 15,4.8 16,4.8Z" />
                
                <path 
                    transform="translate(85,37.5) scale(0.6)" 
                    fill='#1783BB'
                    d="M5,11L6.5,6.5H17.5L19,11M17.5,16A1.5,1.5 0 0,1 16,14.5A1.5,1.5 0 0,1 17.5,13A1.5,1.5 0 0,1 19,14.5A1.5,1.5 0 0,1 17.5,16M6.5,16A1.5,1.5 0 0,1 5,14.5A1.5,1.5 0 0,1 6.5,13A1.5,1.5 0 0,1 8,14.5A1.5,1.5 0 0,1 6.5,16M18.92,6C18.72,5.42 18.16,5 17.5,5H6.5C5.84,5 5.28,5.42 5.08,6L3,12V20A1,1 0 0,0 4,21H5A1,1 0 0,0 6,20V19H18V20A1,1 0 0,0 19,21H20A1,1 0 0,0 21,20V12L18.92,6Z" />
                
            </svg>
            <CityName>{cityname}</CityName>
        </div>
    )
}

const CityName = styled.div`
width: 100%;
text-align: center;
margin-top: 0.2rem;
font-size: 1.3rem;
`

export default SwitchCircle