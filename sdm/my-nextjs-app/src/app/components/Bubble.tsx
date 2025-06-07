"use client"; // This directive marks this component as a Client Component

import React from 'react';

interface DashboardCardProps {
  title: string;
  value: number | string;
  description: string;
}

const Bubble: React.FC<DashboardCardProps> = ({ title, value, description }) => {
  return (
    <div className="bg-white rounded-2xl shadow-xl shadow-gray-300 p-6 w-full sm:w-64">
      <h2 className="text-gray-600 text-sm font-semibold">{title}</h2>
      <p className="text-3xl font-bold text-blue-600 mt-2">{value}</p>
      <p className="text-sm text-gray-500 mt-1">{description}</p>
    </div>
  );
};


export default Bubble;